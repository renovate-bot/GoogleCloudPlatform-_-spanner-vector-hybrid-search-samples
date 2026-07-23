import os
import time
import json
import subprocess
import datetime
import asyncio
from dotenv import load_dotenv

from textual.app import App, ComposeResult
from textual.widgets import Header, Footer, DataTable, RichLog, ContentSwitcher
from textual.containers import Horizontal, Vertical

# Load configuration
load_dotenv("config.env")
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION", "us-central1")
SOURCE_INSTANCE = os.getenv("SOURCE_INSTANCE", "source-instance")
SOURCE_DB = os.getenv("SOURCE_DB", "source-db")
DEST_INSTANCE = os.getenv("DEST_INSTANCE", "dest-instance")
DEST_DB = os.getenv("DEST_DB", "dest-db")

if not PROJECT_ID:
    result = subprocess.run(["gcloud", "config", "get-value", "project"], capture_output=True, text=True)
    PROJECT_ID = result.stdout.strip()

STEPS = [
    (1, "Setup Infrastructure (Terraform)"),
    (2, "Seed Initial Data (Spanner)"),
    (3, "Bulk Export to GCS (Dataflow)"),
    (4, "Bulk Import from GCS (Dataflow)"),
    (5, "CDC Replication (Dataflow)"),
    (6, "Simulate Live Traffic"),
    (7, "Validate Data Consistency"),
    (8, "Cleanup Environment"),
]

class SpannerReplicationDemoApp(App):
    CSS = """
    #top_pane {
        height: 75%;
    }
    DataTable {
        width: 1fr;
        height: 100%;
        border: solid cyan;
    }
    #log_switcher {
        width: 1fr;
        height: 100%;
        border: solid magenta;
    }
    RichLog {
        height: 100%;
    }
    #cmd_log {
        height: 25%;
        border: solid green;
    }
    """
    BINDINGS = [
        ("q", "quit", "Quit")
    ]

    def __init__(self):
        super().__init__()
        self.current_step = 0
        self.step_in_progress = False

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        with Vertical():
            with Horizontal(id="top_pane"):
                yield DataTable(id="steps_table")
                with ContentSwitcher(initial="log_0", id="log_switcher"):
                    yield RichLog(id="log_0", highlight=True, markup=True)
                    for step_id, _ in STEPS:
                        yield RichLog(id=f"log_{step_id}", highlight=True, markup=True)
            yield RichLog(id="cmd_log", highlight=True, markup=True)
        yield Footer()

    def load_state(self):
        try:
            with open(".demo_state.json", "r") as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def save_state(self, step_id, status):
        state = self.load_state()
        state[str(step_id)] = status
        with open(".demo_state.json", "w") as f:
            json.dump(state, f)

    def log_cmd(self, cmd_list):
        if not cmd_list: return
        
        lines = []
        current_line = f"> {cmd_list[0]}"
        
        for arg in cmd_list[1:]:
            if arg.startswith("--"):
                lines.append(current_line)
                current_line = f"    {arg}"
            else:
                current_line += f" {arg}"
                
        lines.append(current_line)
        
        formatted_cmd = " \\\n".join(lines)
        self.query_one("#cmd_log", RichLog).write(f"[bold green]{formatted_cmd}[/bold green]")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "row"
        table.focus()
        table.add_column("Step", key="Step")
        table.add_column("Component", key="Component")
        table.add_column("Status", key="Status", width=20)
        
        state = self.load_state()
        for step_id, name in STEPS:
            status = state.get(str(step_id), "[dim]Pending[/dim]")
            table.add_row(str(step_id), name, status, key=str(step_id))
            
        log = self.query_one("#log_0", RichLog)
        log.write("[bold blue]Welcome to the Spanner Zero-Downtime Migration Demo![/bold blue]")
        log.write("Press [bold green]Enter[/bold green] to begin Step 1.")
        
        cmd_log = self.query_one("#cmd_log", RichLog)
        cmd_log.write("[dim]Command execution log...[/dim]")


    def on_data_table_row_highlighted(self, event: DataTable.RowHighlighted) -> None:
        step_id = event.row_key.value
        self.query_one("#log_switcher", ContentSwitcher).current = f"log_{step_id}"

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        if self.step_in_progress:
            step_id = event.row_key.value
            self.query_one(f"#log_{step_id}", RichLog).write("[yellow]A step is currently in progress. Please wait.[/yellow]")
            return
            
        step_id = int(event.row_key.value)
        self.current_step = step_id
        self.step_in_progress = True
        
        if step_id == 1:
            self.run_worker(self.run_step1, exclusive=True)
        elif step_id == 2:
            self.run_worker(self.run_step2, exclusive=True)
        elif step_id == 3:
            self.run_worker(self.run_step3, exclusive=True)
        elif step_id == 4:
            self.run_worker(self.run_step4, exclusive=True)
        elif step_id == 5:
            self.run_worker(self.run_step5, exclusive=True)
        elif step_id == 6:
            self.run_worker(self.run_step6, exclusive=True)
        elif step_id == 7:
            self.run_worker(self.run_step7, exclusive=True)
        elif step_id == 8:
            self.run_worker(self.run_step8, exclusive=True)

    def update_status(self, step_idx: int, status: str):
        table = self.query_one(DataTable)
        table.update_cell(str(step_idx), "Status", status)
        self.save_state(step_idx, status)

    async def run_cmd_async(self, cmd, log_prefix="", step_id=None):
        if step_id is None: step_id = self.current_step
        log = self.query_one(f"#log_{step_id}", RichLog)
        self.log_cmd(cmd)
        
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT
        )
        
        while True:
            line = await process.stdout.readline()
            if not line:
                break
            text = line.decode('utf-8').strip()
            if text:
                log.write(f"{log_prefix} {text}")
                
        await process.wait()
        return process.returncode

    async def run_step1(self):
        log = self.query_one(f"#log_{self.current_step}", RichLog)
        log.write("\n[bold yellow]--- Starting Step 1: Terraform ---[/bold yellow]")
        self.update_status(1, "[yellow]Initializing...[/yellow]")
        
        await self.run_cmd_async(["terraform", "init", "-no-color"], "[TF Init]")
        
        self.update_status(1, "[yellow]Applying...[/yellow]")
        code = await self.run_cmd_async(["terraform", "apply", "-auto-approve", "-no-color", f"-var=project_id={PROJECT_ID}"], "[TF Apply]")
        
        if code == 0:
            self.update_status(1, "[green]Completed[/green]")
            log.write("[bold green]Step 1 Complete. Press Enter for Step 2.[/bold green]")
        else:
            self.update_status(1, "[red]Failed[/red]")
        self.step_in_progress = False

    async def run_step2(self):
        log = self.query_one(f"#log_{self.current_step}", RichLog)
        log.write("\n[bold yellow]--- Starting Step 2: Seed Data ---[/bold yellow]")
        self.update_status(2, "[yellow]Running...[/yellow]")
        
        code = await self.run_cmd_async(
            ["python3", "bulk_load.py", f"--project={PROJECT_ID}", f"--instance={SOURCE_INSTANCE}", f"--database={SOURCE_DB}"], 
            "[Seeder]"
        )
        
        if code == 0:
            self.update_status(2, "[green]Completed[/green]")
            log.write("[bold green]Step 2 Complete. Press Enter for Step 3.[/bold green]")
        else:
            self.update_status(2, "[red]Failed[/red]")
        self.step_in_progress = False

    async def poll_dataflow_job(self, job_id, step_id, log):
        error_count = 0
        while True:
            status_proc = await asyncio.create_subprocess_exec(
                "gcloud", "dataflow", "jobs", "show", job_id, "--format=json", f"--region={REGION}",
                stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
            )
            s_stdout, s_stderr = await status_proc.communicate()
            if status_proc.returncode == 0:
                error_count = 0  # reset consecutive errors
                try:
                    s_info = json.loads(s_stdout.decode('utf-8'))
                    state = s_info.get("state") or s_info.get("currentState") or "UNKNOWN"
                    log.write(f"Polled job status: {state}")
                    
                    if state in ("JOB_STATE_DONE", "Done", "DONE"):
                        return True
                    elif state in ("JOB_STATE_FAILED", "JOB_STATE_CANCELLED", "Failed", "Cancelled", "FAILED", "CANCELLED"):
                        self.update_status(step_id, f"[red]{state}[/red]")
                        return False
                    else:
                        self.update_status(step_id, f"[blue]{state}[/blue]")
                except Exception as e:
                    log.write(f"[red]Error parsing job status JSON: {e}[/red]")
            else:
                error_count += 1
                log.write(f"[red]Error polling status (Attempt {error_count}/5): {s_stderr.decode('utf-8').strip()}[/red]")
                if error_count >= 5:
                    self.update_status(step_id, "[red]Polling Failed[/red]")
                    return False
            
            await asyncio.sleep(10)

    async def run_step3(self):
        log = self.query_one(f"#log_{self.current_step}", RichLog)
        log.write("\n[bold yellow]--- Starting Step 3: Bulk Export to GCS ---[/bold yellow]")
        
        cmd_tf = ["terraform", "output", "-raw", "dataflow_temp_bucket"]
        self.log_cmd(cmd_tf)
        bucket_proc = await asyncio.create_subprocess_exec(*cmd_tf, stdout=asyncio.subprocess.PIPE)
        stdout, _ = await bucket_proc.communicate()
        temp_bucket = stdout.decode('utf-8').strip()
        
        import uuid
        export_id = uuid.uuid4().hex[:8]
        export_folder = f"{temp_bucket}/export-{export_id}"
        
        self.update_status(3, "[yellow]Exporting...[/yellow]")
        log.write("[blue]Phase 1: Exporting Spanner to GCS[/blue]")
        
        import datetime
        fallback_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat().replace("+00:00", "Z")
        
        runner_mode = os.getenv("DATAFLOW_RUNNER", "remote")
        if runner_mode == "local":
            log.write("[blue]Running Official Template Locally via DirectRunner...[/blue]")
            cmd_export = [
                "bash", "run_local_template.sh", "Cloud_Spanner_to_GCS_Avro",
                f"--project={PROJECT_ID}", f"--instanceId={SOURCE_INSTANCE}", f"--databaseId={SOURCE_DB}", f"--outputDir={export_folder}"
            ]
            code = await self.run_cmd_async(cmd_export, "[Local Export]")
            if code != 0:
                self.update_status(3, "[red]Failed[/red]")
                self.step_in_progress = False
                return
        else:
            cmd_export = [
                "gcloud", "dataflow", "jobs", "run", f"spanner-export-{export_id}",
                f"--gcs-location=gs://dataflow-templates-{REGION}/latest/Cloud_Spanner_to_GCS_Avro",
                f"--region={REGION}",
                f"--parameters=instanceId={SOURCE_INSTANCE},databaseId={SOURCE_DB},outputDir={export_folder}",
                "--format=json"
            ]
            
            self.log_cmd(cmd_export)
            proc = await asyncio.create_subprocess_exec(*cmd_export, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()
            
            if proc.returncode != 0:
                log.write(f"[red]Export launch failed: {stderr.decode('utf-8')}[/red]")
                self.update_status(3, "[red]Failed[/red]")
                self.step_in_progress = False
                return
                
            try:
                job_info = json.loads(stdout.decode('utf-8'))
                export_job_id = job_info['id']
                log.write(f"[green]Export Job launched! ID: {export_job_id}[/green]")
            except Exception:
                log.write("[red]Failed to parse Export Job ID.[/red]")
                self.update_status(3, "[red]Failed[/red]")
                self.step_in_progress = False
                return
            
            success = await self.poll_dataflow_job(export_job_id, 3, log)
            if not success:
                self.step_in_progress = False
                return
            
        # The Export job creates a timestamped subfolder inside our export_folder.
        # We need to find that exact subfolder to read the JSON and pass to the Import job.
        log.write("[blue]Locating generated export subfolder...[/blue]")
        cmd_ls = ["gcloud", "storage", "ls", f"{export_folder}/"]
        self.log_cmd(cmd_ls)
        ls_proc = await asyncio.create_subprocess_exec(
            *cmd_ls,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        ls_stdout, ls_stderr = await ls_proc.communicate()
        
        if ls_proc.returncode != 0:
            log.write(f"[red]Failed to list export directory: {ls_stderr.decode('utf-8')}[/red]")
            self.update_status(3, "[red]Failed[/red]")
            self.step_in_progress = False
            return
            
        # Parse the subfolder path from the ls output (it will end in a slash)
        ls_lines = [line.strip() for line in ls_stdout.decode('utf-8').split('\n') if line.strip() and line.strip().endswith('/')]
        if not ls_lines:
            log.write("[red]Could not find any subfolders in the export directory.[/red]")
            self.update_status(3, "[red]Failed[/red]")
            self.step_in_progress = False
            return
            
        true_export_folder = ls_lines[0].rstrip('/')
        log.write(f"[green]Found true export folder: {true_export_folder}[/green]")
        
        # Extract snapshot time from spanner-export.json
        log.write("[blue]Extracting snapshotTime from spanner-export.json...[/blue]")
        
        cmd_cat = ["gcloud", "storage", "cat", f"{true_export_folder}/spanner-export.json"]
        self.log_cmd(cmd_cat)
        cat_proc = await asyncio.create_subprocess_exec(
            *cmd_cat,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        cat_stdout, cat_stderr = await cat_proc.communicate()
        
        true_snapshot_time = None
        if cat_proc.returncode == 0:
            try:
                export_manifest = json.loads(cat_stdout.decode('utf-8'))
                true_snapshot_time = export_manifest.get('snapshotTime')
            except Exception:
                pass
                
        if true_snapshot_time:
            log.write(f"[green]Captured True Snapshot Timestamp: {true_snapshot_time}[/green]")
            final_timestamp = true_snapshot_time
        else:
            log.write(f"[yellow]snapshotTime not found. Falling back to pre-job timestamp: {fallback_timestamp}[/yellow]")
            final_timestamp = fallback_timestamp
            
        with open("start_timestamp.txt", "w") as f:
            f.write(final_timestamp)
        
        with open("export_folder.txt", "w") as f:
            f.write(true_export_folder)
            
        self.update_status(3, "[green]Completed[/green]")
        log.write("\n[bold green]Step 3 Complete. Press Enter on Step 4 to continue.[/bold green]")
        self.step_in_progress = False

    async def run_step4(self):
        log = self.query_one(f"#log_{self.current_step}", RichLog)
        log.write("\n[bold yellow]--- Starting Step 4: Bulk Import from GCS ---[/bold yellow]")
        
        try:
            with open("export_folder.txt", "r") as f:
                export_folder = f.read().strip()
        except Exception:
            log.write("[red]export_folder.txt not found! Run Step 3 first.[/red]")
            self.update_status(4, "[red]Failed[/red]")
            self.step_in_progress = False
            return
            
        self.update_status(4, "[yellow]Importing...[/yellow]")
        
        import uuid
        import_id = uuid.uuid4().hex[:8]
        
        runner_mode = os.getenv("DATAFLOW_RUNNER", "remote")
        
        if runner_mode == "local":
            log.write("[blue]Running Official Template Locally via DirectRunner...[/blue]")
            cmd_import = [
                "bash", "run_local_template.sh", "GCS_Avro_to_Cloud_Spanner",
                f"--project={PROJECT_ID}", f"--instanceId={DEST_INSTANCE}", f"--databaseId={DEST_DB}", f"--inputDir={export_folder}/"
            ]
            code = await self.run_cmd_async(cmd_import, "[Local Import]")
            if code == 0:
                self.update_status(4, "[green]Completed[/green]")
                log.write("\n[bold green]Step 4 Complete. Press Enter for Step 5.[/bold green]")
            else:
                self.update_status(4, "[red]Failed[/red]")
        else:
            cmd_import = [
                "gcloud", "dataflow", "jobs", "run", f"spanner-import-{import_id}",
                f"--gcs-location=gs://dataflow-templates-{REGION}/latest/GCS_Avro_to_Cloud_Spanner",
                f"--region={REGION}",
                f"--parameters=instanceId={DEST_INSTANCE},databaseId={DEST_DB},inputDir={export_folder}/",
                "--format=json"
            ]
            
            self.log_cmd(cmd_import)
            proc2 = await asyncio.create_subprocess_exec(*cmd_import, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout2, stderr2 = await proc2.communicate()
            
            if proc2.returncode != 0:
                log.write(f"[red]Import launch failed: {stderr2.decode('utf-8')}[/red]")
                self.update_status(4, "[red]Failed[/red]")
                self.step_in_progress = False
                return
                
            try:
                job_info2 = json.loads(stdout2.decode('utf-8'))
                import_job_id = job_info2['id']
                log.write(f"[green]Import Job launched! ID: {import_job_id}[/green]")
            except Exception:
                log.write("[red]Failed to parse Import Job ID.[/red]")
                self.update_status(4, "[red]Failed[/red]")
                self.step_in_progress = False
                return
            
            success = await self.poll_dataflow_job(import_job_id, 4, log)
            if success:
                self.update_status(4, "[green]Completed[/green]")
                log.write("\n[bold green]Step 4 Complete. Press Enter for Step 5.[/bold green]")
            
        self.step_in_progress = False

    async def run_step5(self):
        log = self.query_one(f"#log_{self.current_step}", RichLog)
        log.write("\n[bold yellow]--- Starting Step 5: CDC Replication ---[/bold yellow]")
        
        try:
            with open("start_timestamp.txt", "r") as f:
                start_timestamp = f.read().strip()
                if start_timestamp.endswith("+00:00Z"):
                    start_timestamp = start_timestamp.replace("+00:00Z", "Z")
        except Exception:
            log.write("[red]start_timestamp.txt not found![/red]")
            self.update_status(5, "[red]Failed[/red]")
            self.step_in_progress = False
            return

        self.update_status(5, "[yellow]Launching...[/yellow]")
        
        cmd_tf = ["terraform", "output", "-raw", "dataflow_temp_bucket"]
        self.log_cmd(cmd_tf)
        bucket_proc = await asyncio.create_subprocess_exec(*cmd_tf, stdout=asyncio.subprocess.PIPE)
        stdout, _ = await bucket_proc.communicate()
        temp_bucket = stdout.decode('utf-8').strip()
        
        config_json_content = f"""[
  {{
    "projectId": "{PROJECT_ID}",
    "instanceId": "{DEST_INSTANCE}",
    "databaseId": "{DEST_DB}"
  }}
]"""
        config_path = "spanner_shard_config.json"
        with open(config_path, "w") as f:
            f.write(config_json_content)
            
        shards_gcs_path = f"{temp_bucket}/config.json"
        log.write("[yellow]Uploading shard config to GCS...[/yellow]")
        await self.run_cmd_async(["gcloud", "storage", "cp", config_path, shards_gcs_path], "[Config]")

        args = [
            f"--changeStreamName=streamall",
            f"--instanceId={SOURCE_INSTANCE}",
            f"--databaseId={SOURCE_DB}",
            f"--sourceType=spanner",
            f"--spannerProjectId={PROJECT_ID}",
            f"--metadataInstance={DEST_INSTANCE}",
            f"--metadataDatabase={DEST_DB}",
            f"--sourceShardsFilePath={shards_gcs_path}",
            f"--deadLetterQueueDirectory={temp_bucket}/dlq"
        ]
        
        if start_timestamp:
            args.append(f"--startTimestamp={start_timestamp}")

        runner_mode = os.getenv("DATAFLOW_RUNNER", "remote")
        
        if runner_mode == "local":
            log.write("[blue]Running Official Template Locally via DirectRunner...[/blue]")
            local_args = args + [
                f"--tempLocation={temp_bucket}/temp",
                "--workerMachineType=n2-standard-4"
            ]
            cmd = ["bash", "run_local_template.sh", "spanner-to-sourcedb"] + local_args
            asyncio.create_task(self.run_cmd_async(cmd, "[Local CDC]"))
            self.update_status(5, "[green]Running[/green]")
            log.write("\n[bold green]Step 5 CDC Pipeline is streaming in the background![/bold green]")
            log.write("[bold green]Press Enter to proceed to Step 6.[/bold green]")
        else:
            param_string = ",".join(args).replace("--", "")
            cmd = [
                "gcloud", "dataflow", "flex-template", "run", "cdc-replication-job",
                f"--project={PROJECT_ID}", f"--region={REGION}",
                f"--template-file-gcs-location=gs://dataflow-templates-{REGION}/latest/flex/Spanner_to_SourceDb",
                f"--parameters={param_string}",
                f"--temp-location={temp_bucket}/temp",
                f"--worker-machine-type=n2-standard-4",
                f"--max-workers=5",
                f"--staging-location={temp_bucket}/staging", "--format=json"
            ]
            
            self.log_cmd(cmd)
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()
            
            if proc.returncode != 0:
                log.write(f"[red]Launch failed: {stderr.decode('utf-8')}[/red]")
                self.update_status(5, "[red]Failed[/red]")
            else:
                try:
                    job_info = json.loads(stdout.decode('utf-8'))
                    job_id = job_info['job']['id']
                    log.write(f"[green]CDC Job running! Job ID: {job_id}[/green]")
                    self.update_status(5, f"[green]Running ({job_id})[/green]")
                except Exception:
                    self.update_status(5, "[green]Running[/green]")
            
            log.write("\n[bold green]Step 5 Complete. Press Enter for Step 6.[/bold green]")
        self.step_in_progress = False

    async def run_step6(self):
        log = self.query_one(f"#log_{self.current_step}", RichLog)
        log.write("\n[bold yellow]--- Starting Step 6: Live Traffic ---[/bold yellow]")
        self.update_status(6, "[yellow]Running...[/yellow]")
        
        log.write("Simulating live user traffic (inserting 100 records)...")
        
        await self.run_cmd_async(
            ["python3", "generate_load.py", f"--project={PROJECT_ID}", f"--instance={SOURCE_INSTANCE}", f"--database={SOURCE_DB}", "--count=100"], 
            "[Live]"
        )
        self.update_status(6, "[green]Completed[/green]")
        log.write("\n[bold green]Step 6 Complete. Press Enter for Step 7.[/bold green]")
        self.step_in_progress = False


    async def run_step7(self):
        log = self.query_one(f"#log_{self.current_step}", RichLog)
        log.write("\n[bold yellow]--- Starting Step 7: Validate Data Consistency ---[/bold yellow]")
        self.update_status(7, "[yellow]Running...[/yellow]")
        
        code = await self.run_cmd_async(
            ["python3", "validate_data.py", 
             f"--project={PROJECT_ID}", 
             f"--source-instance={SOURCE_INSTANCE}", f"--source-database={SOURCE_DB}",
             f"--dest-instance={DEST_INSTANCE}", f"--dest-database={DEST_DB}"], 
            "[Validate]"
        )
        
        if code == 0:
            self.update_status(7, "[green]Completed[/green]")
            log.write("\n[bold green]Step 7 Complete. Data is fully synchronized. Press Enter for Step 8.[/bold green]")
        else:
            self.update_status(7, "[red]Failed[/red]")
            log.write("\n[bold red]Validation failed. Press Enter to retry Step 7![/bold red]")
            
        self.step_in_progress = False

    async def run_step8(self):
        log = self.query_one(f"#log_{self.current_step}", RichLog)
        log.write("\n[bold yellow]--- Starting Step 8: Cleanup Environment ---[/bold yellow]")
        self.update_status(8, "[yellow]Cleaning up...[/yellow]")
        
        code = await self.run_cmd_async(["bash", "cleanup.sh"], "[Cleanup]")
        
        if code == 0:
            self.update_status(8, "[green]Completed[/green]")
            log.write("[bold green]Step 8 Complete. Environment torn down.[/bold green]")
            # Reset UI table state since cleanup nuked the state file
            table = self.query_one(DataTable)
            for step_id in range(1, 9):
                table.update_cell(str(step_id), "Status", "[dim]Pending[/dim]")
        else:
            self.update_status(8, "[red]Failed[/red]")
        self.step_in_progress = False

if __name__ == "__main__":
    app = SpannerReplicationDemoApp()
    app.run()
