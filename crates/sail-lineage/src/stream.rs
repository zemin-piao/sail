use std::pin::Pin;
use std::task::{Context, Poll};

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;

use crate::service::LineageRun;

/// A record batch stream that completes the lineage run when the stream ends.
///
/// The run is marked as complete when the stream is exhausted, and as failed
/// when the stream returns an error. If the stream is dropped before either happens
/// (e.g. the query is cancelled), the run is aborted.
pub struct LineageStream {
    inner: SendableRecordBatchStream,
    run: Option<LineageRun>,
}

impl LineageStream {
    pub fn new(inner: SendableRecordBatchStream, run: LineageRun) -> Self {
        Self {
            inner,
            run: Some(run),
        }
    }
}

impl Stream for LineageStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let poll = self.inner.as_mut().poll_next(cx);
        match &poll {
            Poll::Ready(None) => {
                if let Some(run) = self.run.take() {
                    run.complete();
                }
            }
            Poll::Ready(Some(Err(e))) => {
                if let Some(run) = self.run.take() {
                    run.fail(e.to_string());
                }
            }
            Poll::Ready(Some(Ok(_))) | Poll::Pending => {}
        }
        poll
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RecordBatchStream for LineageStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::common::DataFusionError;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use datafusion::logical_expr::{LogicalPlanBuilder, TableSource};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use futures::StreamExt;
    use serde_json::Value;
    use tokio::sync::mpsc;
    use uuid::Uuid;

    use super::*;
    use crate::dataset::LineageDataset;
    use crate::event::LifecycleStateChange;
    use crate::options::{LineageSessionOptions, RunReference};
    use crate::plan::{LineageOperation, LineagePlan};
    use crate::service::LineageService;

    const SESSION_ID: &str = "session";

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn source() -> Arc<dyn TableSource> {
        provider_as_source(Arc::new(EmptyTable::new(schema())))
    }

    fn stream(items: Vec<Result<RecordBatch>>) -> SendableRecordBatchStream {
        Box::pin(RecordBatchStreamAdapter::new(
            schema(),
            futures::stream::iter(items),
        ))
    }

    fn received_all(receiver: &mut mpsc::Receiver<String>) -> Vec<Value> {
        let mut events = vec![];
        while let Ok(event) = receiver.try_recv() {
            events.push(serde_json::from_str(&event).unwrap_or_default());
        }
        events
    }

    /// Returns the received query run events, excluding the application run events.
    fn received(receiver: &mut mpsc::Receiver<String>) -> Vec<Value> {
        received_all(receiver)
            .into_iter()
            .filter(|x| x["job"]["facets"]["jobType"]["jobType"] != "APPLICATION")
            .collect()
    }

    fn event_types(events: &[Value]) -> Vec<Value> {
        events.iter().map(|x| x["eventType"].clone()).collect()
    }

    fn write_plan(service: &LineageService, source: Arc<dyn TableSource>) -> Result<LineagePlan> {
        let logical = LogicalPlanBuilder::scan("t", source, None)?.build()?;
        Ok(LineagePlan {
            operation: LineageOperation::Write,
            inputs: service.find_inputs(&logical),
            outputs: vec![LineageDataset {
                location: Some("/data/out".to_string()),
                lifecycle: Some(LifecycleStateChange::Overwrite),
                ..Default::default()
            }],
            streaming: false,
        })
    }

    /// Runs the plan with a stream of the given items until the end of the stream.
    async fn run(
        service: &Arc<LineageService>,
        options: &LineageSessionOptions,
        plan: LineagePlan,
        items: Vec<Result<RecordBatch>>,
    ) {
        let Some(run) = service.start_run(SESSION_ID, options, plan) else {
            return;
        };
        let mut stream = LineageStream::new(stream(items), run);
        while stream.next().await.is_some() {}
    }

    #[tokio::test]
    async fn test_complete() -> Result<()> {
        let (sender, mut receiver) = mpsc::channel(16);
        let service = Arc::new(LineageService::with_sender("sail", sender));
        let source = source();
        service.register_source(
            &source,
            vec![LineageDataset {
                location: Some("/data/in".to_string()),
                schema: Some(schema()),
                ..Default::default()
            }],
        );
        let options = LineageSessionOptions {
            app_name: Some("app".to_string()),
            ..Default::default()
        };
        run(&service, &options, write_plan(&service, source)?, vec![]).await;

        let events = received(&mut receiver);
        assert_eq!(event_types(&events), vec!["START", "COMPLETE"]);
        assert_eq!(events[0]["run"]["runId"], events[1]["run"]["runId"]);
        assert_eq!(events[0]["job"]["namespace"], "sail");
        assert_eq!(events[0]["job"]["name"], "app.write.data_out");
        assert_eq!(events[0]["job"]["facets"]["jobType"]["jobType"], "WRITE");
        assert_eq!(events[0]["inputs"][0]["name"], "/data/in");
        assert_eq!(
            events[0]["inputs"][0]["facets"]["schema"]["fields"][0]["name"],
            "id"
        );
        assert_eq!(
            events[0]["outputs"][0]["facets"]["lifecycleStateChange"]["lifecycleStateChange"],
            "OVERWRITE"
        );
        // The query run is a child of the application run of the session.
        assert_eq!(events[0]["run"]["facets"]["parent"]["job"]["name"], "app");
        Ok(())
    }

    #[tokio::test]
    async fn test_fail_and_abort() -> Result<()> {
        let (sender, mut receiver) = mpsc::channel(16);
        let service = Arc::new(LineageService::with_sender("sail", sender));
        let options = LineageSessionOptions::default();

        let error = Err(DataFusionError::Execution("boom".to_string()));
        run(
            &service,
            &options,
            write_plan(&service, source())?,
            vec![error],
        )
        .await;
        let events = received(&mut receiver);
        assert_eq!(event_types(&events), vec!["START", "FAIL"]);
        assert_eq!(
            events[1]["run"]["facets"]["errorMessage"]["message"],
            "Execution error: boom"
        );
        assert_eq!(events[0]["job"]["name"], "sail.write.data_out");

        // The stream is dropped before it ends.
        let run = service.start_run(SESSION_ID, &options, write_plan(&service, source())?);
        drop(run.map(|x| LineageStream::new(stream(vec![]), x)));
        assert_eq!(
            event_types(&received(&mut receiver)),
            vec!["START", "ABORT"]
        );

        // A plan without any dataset does not start a run.
        assert!(
            service
                .start_run(SESSION_ID, &options, LineagePlan::default())
                .is_none()
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_parent_run_and_namespace() -> Result<()> {
        let (sender, mut receiver) = mpsc::channel(16);
        let service = Arc::new(LineageService::with_sender("sail", sender));
        let reference = |name: &str| RunReference {
            namespace: "airflow".to_string(),
            name: name.to_string(),
            run_id: Uuid::nil(),
        };
        let options = LineageSessionOptions {
            namespace: Some("team".to_string()),
            parent: Some(reference("dag.task")),
            root_parent: Some(reference("dag")),
            ..Default::default()
        };
        run(&service, &options, write_plan(&service, source())?, vec![]).await;

        let events = received_all(&mut receiver);
        assert_eq!(events.len(), 3);
        // The application run is a child of the orchestrator run.
        let application = &events[0];
        assert_eq!(application["job"]["namespace"], "team");
        assert_eq!(application["job"]["name"], "sail");
        let parent = &application["run"]["facets"]["parent"];
        assert_eq!(parent["run"]["runId"], Uuid::nil().to_string());
        assert_eq!(parent["job"]["namespace"], "airflow");
        assert_eq!(parent["job"]["name"], "dag.task");
        assert_eq!(parent["root"]["job"]["name"], "dag");
        // The query run is a child of the application run, with the same root.
        for event in &events[1..] {
            assert_eq!(event["job"]["namespace"], "team");
            let parent = &event["run"]["facets"]["parent"];
            assert_eq!(parent["run"]["runId"], application["run"]["runId"]);
            assert_eq!(parent["job"]["namespace"], "team");
            assert_eq!(parent["job"]["name"], "sail");
            assert_eq!(parent["root"]["job"]["namespace"], "airflow");
            assert_eq!(parent["root"]["job"]["name"], "dag");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_tags() -> Result<()> {
        let (sender, mut receiver) = mpsc::channel(16);
        let service = Arc::new(LineageService::with_sender("sail", sender));
        let options = LineageSessionOptions::from_spark_config(|key| match key {
            "spark.openlineage.job.tags" => Some("dag_id:d;task_id:t".to_string()),
            "spark.openlineage.run.tags" => Some("try_number:2".to_string()),
            _ => None,
        });
        run(&service, &options, write_plan(&service, source())?, vec![]).await;

        // Both the application run and the query run have the tags.
        let events = received_all(&mut receiver);
        assert_eq!(events.len(), 3);
        for event in &events {
            let job_tags = &event["job"]["facets"]["tags"]["tags"];
            assert_eq!(job_tags[0]["key"], "dag_id");
            assert_eq!(job_tags[0]["value"], "d");
            assert_eq!(job_tags[0]["source"], "CONFIG");
            assert_eq!(job_tags[1]["key"], "task_id");
            let run_tags = &event["run"]["facets"]["tags"]["tags"];
            assert_eq!(run_tags[0]["key"], "try_number");
            assert_eq!(run_tags[0]["value"], "2");
        }
        Ok(())
    }

    #[test]
    fn test_aliased_source() -> Result<()> {
        let (sender, _receiver) = mpsc::channel(1);
        let service = LineageService::with_sender("sail", sender);
        let (original, wrapper) = (source(), source());
        let logical = LogicalPlanBuilder::scan("t", Arc::clone(&wrapper), None)?.build()?;
        assert!(service.find_inputs(&logical).is_empty());
        service.register_source(
            &original,
            vec![LineageDataset {
                location: Some("/data/in".to_string()),
                ..Default::default()
            }],
        );
        service.register_source_alias(&original, &wrapper);
        assert_eq!(service.find_inputs(&logical).len(), 1);
        Ok(())
    }
}
