use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use chrono::Utc;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::logical_expr::{LogicalPlan, TableSource};
use log::{debug, warn};
use sail_common::config::LineageConfig;
use sail_common::runtime::RuntimeHandle;
use sail_common_datafusion::extension::SessionExtension;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use uuid::Uuid;

use crate::dataset::LineageDataset;
use crate::error::LineageResult;
use crate::event::{EventType, InputDataset, Job, OutputDataset, ProcessingType, Run, RunEvent};
use crate::options::{LineageSessionOptions, LineageTag, RunReference};
use crate::plan::LineagePlan;
use crate::transport::Transport;

/// The maximum number of events waiting to be sent.
/// Events are dropped when the queue is full, so that query execution is never blocked.
const QUEUE_SIZE: usize = 1024;

/// The job name prefix when the client does not provide an application name.
const DEFAULT_APP_NAME: &str = "sail";

/// The job type of application runs, which matches the OpenLineage Spark integration.
const APPLICATION_JOB_TYPE: &str = "APPLICATION";

struct RegisteredSource {
    source: Weak<dyn TableSource>,
    datasets: Vec<LineageDataset>,
}

/// A service that tracks datasets and emits OpenLineage run events.
/// The service is only registered as a session extension when lineage is enabled.
///
/// The service is shared by all sessions of a server, so that tables read via
/// global temporary views can be traced to their underlying datasets.
pub struct LineageService {
    namespace: String,
    shutdown_timeout: Duration,
    /// The sender is removed when the service shuts down, which closes the queue
    /// so that the delivery task exits after sending the remaining events.
    sender: Mutex<Option<mpsc::Sender<String>>>,
    delivery: Mutex<Option<JoinHandle<()>>>,
    /// Data sources created during plan resolution, keyed by the address of the
    /// [`TableSource`]. Each entry holds a weak reference so that the registry
    /// does not keep sources alive after their plans are dropped.
    sources: Mutex<HashMap<usize, RegisteredSource>>,
    /// The application runs of active sessions, keyed by session ID.
    applications: Mutex<HashMap<String, ApplicationRun>>,
}

impl SessionExtension for LineageService {
    fn name() -> &'static str {
        "LineageService"
    }
}

impl LineageService {
    /// Creates a service and spawns the task that sends events on the I/O runtime.
    /// Returns [`None`] if lineage is disabled.
    pub fn try_from_config(
        config: &LineageConfig,
        runtime: &RuntimeHandle,
    ) -> LineageResult<Option<Arc<Self>>> {
        if !config.enabled {
            return Ok(None);
        }
        let transport = Transport::try_new(config, runtime)?;
        let (sender, receiver) = mpsc::channel(QUEUE_SIZE);
        let delivery = runtime.io().spawn(deliver(transport, receiver));
        Ok(Some(Arc::new(Self::new(
            &config.namespace,
            Duration::from_secs(config.shutdown_timeout_secs),
            sender,
            Some(delivery),
        ))))
    }

    fn new(
        namespace: &str,
        shutdown_timeout: Duration,
        sender: mpsc::Sender<String>,
        delivery: Option<JoinHandle<()>>,
    ) -> Self {
        Self {
            namespace: namespace.to_string(),
            shutdown_timeout,
            sender: Mutex::new(Some(sender)),
            delivery: Mutex::new(delivery),
            sources: Mutex::new(HashMap::new()),
            applications: Mutex::new(HashMap::new()),
        }
    }

    #[cfg(test)]
    pub(crate) fn with_sender(namespace: &str, sender: mpsc::Sender<String>) -> Self {
        Self::new(namespace, Duration::from_secs(1), sender, None)
    }

    fn source_key(source: &Arc<dyn TableSource>) -> usize {
        Arc::as_ptr(source) as *const () as usize
    }

    /// Records the datasets that a data source reads from.
    pub fn register_source(&self, source: &Arc<dyn TableSource>, datasets: Vec<LineageDataset>) {
        if datasets.is_empty() {
            return;
        }
        let Ok(mut sources) = self.sources.lock() else {
            return;
        };
        // The number of live sources is small, so it is cheap to prune on every insert.
        sources.retain(|_, x| x.source.strong_count() > 0);
        sources.insert(
            Self::source_key(source),
            RegisteredSource {
                source: Arc::downgrade(source),
                datasets,
            },
        );
    }

    /// Records that `wrapper` reads from the same datasets as `source`.
    pub fn register_source_alias(
        &self,
        source: &Arc<dyn TableSource>,
        wrapper: &Arc<dyn TableSource>,
    ) {
        if let Some(datasets) = self.find_source(source) {
            self.register_source(wrapper, datasets);
        }
    }

    fn find_source(&self, source: &Arc<dyn TableSource>) -> Option<Vec<LineageDataset>> {
        let sources = self.sources.lock().ok()?;
        let entry = sources.get(&Self::source_key(source))?;
        // The address may have been reused by another source after the original
        // one was dropped, so we must check that the entry refers to the same source.
        let registered = entry.source.upgrade()?;
        Arc::ptr_eq(&registered, source).then(|| entry.datasets.clone())
    }

    /// Returns the datasets read by the logical plan.
    pub fn find_inputs(&self, plan: &LogicalPlan) -> Vec<LineageDataset> {
        let mut inputs = vec![];
        let _ = plan.apply_with_subqueries(|node| {
            if let LogicalPlan::TableScan(scan) = node
                && let Some(datasets) = self.find_source(&scan.source)
            {
                inputs.extend(datasets);
            }
            Ok(TreeNodeRecursion::Continue)
        });
        inputs
    }

    fn job_namespace<'a>(&'a self, options: &'a LineageSessionOptions) -> &'a str {
        options.namespace.as_deref().unwrap_or(&self.namespace)
    }

    /// Starts a run for the plan and emits the `START` event.
    /// The run is a child of the application run of the session,
    /// which is started if this is the first run of the session.
    /// Returns [`None`] if the plan does not involve any dataset.
    pub fn start_run(
        self: &Arc<Self>,
        session_id: &str,
        options: &LineageSessionOptions,
        plan: LineagePlan,
    ) -> Option<LineageRun> {
        if plan.is_empty() {
            return None;
        }
        let (parent, root) = match self.start_application(session_id, options) {
            // The root is the outermost run (e.g. the orchestrator run) that the
            // application is part of, or the application itself.
            Some(application) => {
                let root = options
                    .root_parent
                    .clone()
                    .or_else(|| options.parent.clone())
                    .unwrap_or_else(|| application.clone());
                (Some(application), Some(root))
            }
            None => (options.parent.clone(), options.root_parent.clone()),
        };
        let (inputs, outputs) = plan.datasets(&self.namespace);
        let app_name = options.app_name.as_deref().unwrap_or(DEFAULT_APP_NAME);
        let processing_type = if plan.streaming {
            ProcessingType::Streaming
        } else {
            ProcessingType::Batch
        };
        let job = Job::new(
            self.job_namespace(options),
            plan.job_name(app_name, &self.namespace),
        )
        .with_job_type(processing_type, &plan.operation.as_str().to_uppercase())
        .with_tags(&options.job_tags);
        let run = LineageRun {
            service: Arc::clone(self),
            run_id: Uuid::new_v4(),
            run_tags: options.run_tags.clone(),
            parent,
            root,
            job,
            inputs,
            outputs,
            finished: false,
        };
        run.emit(EventType::Start, None);
        Some(run)
    }

    /// Returns the application run of the session, and emits the `START` event
    /// of the application run if the run does not exist yet.
    /// The application run uses the session options at the time it is started.
    fn start_application(
        &self,
        session_id: &str,
        options: &LineageSessionOptions,
    ) -> Option<RunReference> {
        let mut applications = self.applications.lock().ok()?;
        if let Some(application) = applications.get(session_id) {
            return Some(application.reference());
        }
        let job = Job::new(
            self.job_namespace(options),
            options.app_name.as_deref().unwrap_or(DEFAULT_APP_NAME),
        )
        .with_job_type(ProcessingType::Batch, APPLICATION_JOB_TYPE)
        .with_tags(&options.job_tags);
        let application = ApplicationRun {
            run_id: Uuid::new_v4(),
            job,
            options: options.clone(),
        };
        let reference = application.reference();
        let event = application.event(EventType::Start);
        applications.insert(session_id.to_string(), application);
        drop(applications);
        self.emit(&event);
        Some(reference)
    }

    /// Completes the application run of the session, if the session has started any run.
    pub fn finish_application(&self, session_id: &str) {
        let application = self
            .applications
            .lock()
            .ok()
            .and_then(|mut x| x.remove(session_id));
        if let Some(application) = application {
            self.emit(&application.event(EventType::Complete));
        }
    }

    /// Completes all application runs and waits for the queued events to be sent,
    /// up to the configured shutdown timeout.
    /// Events emitted after shutdown are dropped.
    pub async fn shutdown(&self) {
        let applications = self
            .applications
            .lock()
            .map(|mut x| std::mem::take(&mut *x))
            .unwrap_or_default();
        for application in applications.into_values() {
            self.emit(&application.event(EventType::Complete));
        }
        // Dropping the sender closes the queue once the remaining events are received.
        drop(self.sender.lock().ok().and_then(|mut x| x.take()));
        let delivery = self.delivery.lock().ok().and_then(|mut x| x.take());
        if let Some(delivery) = delivery {
            match tokio::time::timeout(self.shutdown_timeout, delivery).await {
                Ok(Ok(())) => debug!("sent all queued OpenLineage events"),
                Ok(Err(e)) => warn!("failed to send queued OpenLineage events: {e}"),
                Err(_) => warn!(
                    "dropped queued OpenLineage events after waiting for {:?}",
                    self.shutdown_timeout
                ),
            }
        }
    }

    fn emit(&self, event: &RunEvent) {
        let event = match serde_json::to_string(event) {
            Ok(x) => x,
            Err(e) => {
                warn!("failed to serialize OpenLineage event: {e}");
                return;
            }
        };
        let Ok(sender) = self.sender.lock() else {
            return;
        };
        let Some(sender) = sender.as_ref() else {
            debug!("dropped OpenLineage event after shutdown");
            return;
        };
        if let Err(e) = sender.try_send(event) {
            warn!("dropped OpenLineage event: {e}");
        }
    }
}

async fn deliver(transport: Transport, mut receiver: mpsc::Receiver<String>) {
    while let Some(event) = receiver.recv().await {
        match transport.send(&event).await {
            Ok(()) => debug!("emitted OpenLineage event: {event}"),
            Err(e) => warn!("failed to emit OpenLineage event: {e}"),
        }
    }
}

/// An OpenLineage run for a session, which is the parent of the runs
/// of the queries executed in the session.
struct ApplicationRun {
    run_id: Uuid,
    job: Job,
    options: LineageSessionOptions,
}

impl ApplicationRun {
    fn reference(&self) -> RunReference {
        RunReference {
            namespace: self.job.namespace.clone(),
            name: self.job.name.clone(),
            run_id: self.run_id,
        }
    }

    fn event(&self, event_type: EventType) -> RunEvent {
        let mut run = Run::new(self.run_id)
            .with_processing_engine()
            .with_tags(&self.options.run_tags);
        if let Some(parent) = &self.options.parent {
            run = run.with_parent(parent, self.options.root_parent.as_ref());
        }
        RunEvent::new(
            event_type,
            Utc::now(),
            run,
            self.job.clone(),
            vec![],
            vec![],
        )
    }
}

/// An OpenLineage run for a single query execution.
///
/// The run emits an `ABORT` event when it is dropped before
/// it is marked as complete or failed.
pub struct LineageRun {
    service: Arc<LineageService>,
    run_id: Uuid,
    run_tags: Vec<LineageTag>,
    parent: Option<RunReference>,
    root: Option<RunReference>,
    job: Job,
    inputs: Vec<InputDataset>,
    outputs: Vec<OutputDataset>,
    finished: bool,
}

impl LineageRun {
    fn emit(&self, event_type: EventType, error: Option<String>) {
        let mut run = Run::new(self.run_id)
            .with_processing_engine()
            .with_tags(&self.run_tags);
        if let Some(parent) = &self.parent {
            run = run.with_parent(parent, self.root.as_ref());
        }
        if let Some(error) = error {
            run = run.with_error_message(error);
        }
        let event = RunEvent::new(
            event_type,
            Utc::now(),
            run,
            self.job.clone(),
            self.inputs.clone(),
            self.outputs.clone(),
        );
        self.service.emit(&event);
    }

    pub fn complete(mut self) {
        self.finished = true;
        self.emit(EventType::Complete, None);
    }

    pub fn fail(mut self, error: impl Into<String>) {
        self.finished = true;
        self.emit(EventType::Fail, Some(error.into()));
    }
}

impl Drop for LineageRun {
    fn drop(&mut self) {
        if !self.finished {
            self.emit(EventType::Abort, None);
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::Schema;
    use datafusion::datasource::empty::EmptyTable;
    use datafusion::datasource::provider_as_source;
    use serde_json::Value;

    use super::*;
    use crate::plan::LineageOperation;

    fn source() -> Arc<dyn TableSource> {
        provider_as_source(Arc::new(EmptyTable::new(Arc::new(Schema::empty()))))
    }

    fn dataset() -> Vec<LineageDataset> {
        vec![LineageDataset {
            location: Some("/data".to_string()),
            ..Default::default()
        }]
    }

    fn query_plan() -> LineagePlan {
        LineagePlan {
            operation: LineageOperation::Query,
            inputs: dataset(),
            ..Default::default()
        }
    }

    fn received(receiver: &mut mpsc::Receiver<String>) -> Vec<Value> {
        let mut events = vec![];
        while let Ok(event) = receiver.try_recv() {
            events.push(serde_json::from_str(&event).unwrap_or_default());
        }
        events
    }

    #[test]
    fn test_dropped_sources_are_pruned() {
        let (sender, _receiver) = mpsc::channel(1);
        let service = LineageService::with_sender("sail", sender);
        let live = source();
        service.register_source(&live, dataset());
        for _ in 0..10 {
            service.register_source(&source(), dataset());
        }
        let len = service.sources.lock().map(|x| x.len()).unwrap_or_default();
        // The live source and the last dropped source remain.
        assert_eq!(len, 2);
        assert!(service.find_source(&live).is_some());
    }

    #[test]
    fn test_application_run_per_session() {
        let (sender, mut receiver) = mpsc::channel(16);
        let service = Arc::new(LineageService::with_sender("sail", sender));
        let options = LineageSessionOptions {
            app_name: Some("app".to_string()),
            ..Default::default()
        };
        if let Some(run) = service.start_run("s1", &options, query_plan()) {
            run.complete();
        }
        if let Some(run) = service.start_run("s1", &options, query_plan()) {
            run.complete();
        }
        // A plan without any dataset does not start the application run.
        assert!(
            service
                .start_run("s2", &options, LineagePlan::default())
                .is_none()
        );
        service.finish_application("s1");
        // Finishing a session without an application run does nothing.
        service.finish_application("s2");

        let events = received(&mut receiver);
        let summary = events
            .iter()
            .map(|x| {
                (
                    x["eventType"].as_str().unwrap_or_default().to_string(),
                    x["job"]["facets"]["jobType"]["jobType"]
                        .as_str()
                        .unwrap_or_default()
                        .to_string(),
                )
            })
            .collect::<Vec<_>>();
        let pair = |a: &str, b: &str| (a.to_string(), b.to_string());
        assert_eq!(
            summary,
            vec![
                pair("START", "APPLICATION"),
                pair("START", "QUERY"),
                pair("COMPLETE", "QUERY"),
                pair("START", "QUERY"),
                pair("COMPLETE", "QUERY"),
                pair("COMPLETE", "APPLICATION"),
            ]
        );
        let application = &events[0];
        assert_eq!(application["job"]["name"], "app");
        assert!(application["run"]["facets"].get("parent").is_none());
        for event in &events[1..5] {
            let parent = &event["run"]["facets"]["parent"];
            assert_eq!(parent["run"]["runId"], application["run"]["runId"]);
            assert_eq!(parent["job"]["name"], "app");
            // Without an orchestrator parent, the application is the root.
            assert_eq!(parent["root"]["run"]["runId"], application["run"]["runId"]);
        }
        assert_eq!(events[5]["run"]["runId"], application["run"]["runId"]);
    }

    #[tokio::test]
    async fn test_shutdown_completes_applications_and_drains_queue() {
        let (sender, mut receiver) = mpsc::channel::<String>(16);
        let collector = tokio::spawn(async move {
            let mut events = vec![];
            while let Some(event) = receiver.recv().await {
                events.push(serde_json::from_str::<Value>(&event).unwrap_or_default());
            }
            events
        });
        let service = Arc::new(LineageService::new(
            "sail",
            Duration::from_secs(5),
            sender,
            None,
        ));
        if let Some(run) = service.start_run("s1", &LineageSessionOptions::default(), query_plan())
        {
            run.complete();
        }
        service.shutdown().await;
        // Events emitted after shutdown are dropped.
        drop(service.start_run("s2", &LineageSessionOptions::default(), query_plan()));

        let events = collector.await.unwrap_or_default();
        let types = events
            .iter()
            .map(|x| x["eventType"].as_str().unwrap_or_default().to_string())
            .collect::<Vec<_>>();
        assert_eq!(types, vec!["START", "START", "COMPLETE", "COMPLETE"]);
        assert_eq!(
            events[3]["job"]["facets"]["jobType"]["jobType"],
            APPLICATION_JOB_TYPE
        );
    }
}
