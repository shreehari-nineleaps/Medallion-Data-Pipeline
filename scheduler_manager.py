"""
Scheduler Manager for Medallion Data Pipeline
Handles persistent scheduling configuration and job management
"""

import json
import logging
from pathlib import Path
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
import atexit
import sys
import os

# Add project root to path
sys.path.append(str(Path(__file__).parent))
from config import DB_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class SchedulerManager:
    """Manages persistent scheduling for the Medallion Data Pipeline"""

    def __init__(self):
        self.config_file = Path(__file__).parent / 'scheduler_config.json'
        self.scheduler = None
        self.jobs_config = []
        self.load_config()
        self.initialize_scheduler()

    def load_config(self):
        """Load scheduler configuration from file"""
        try:
            if self.config_file.exists():
                with open(self.config_file, 'r') as f:
                    self.jobs_config = json.load(f)
                logger.info(f"Loaded {len(self.jobs_config)} scheduled jobs from config")
            else:
                self.jobs_config = []
                logger.info("No existing scheduler config found, starting fresh")
        except Exception as e:
            logger.error(f"Error loading scheduler config: {e}")
            self.jobs_config = []

    def save_config(self):
        """Save scheduler configuration to file"""
        try:
            with open(self.config_file, 'w') as f:
                json.dump(self.jobs_config, f, indent=2, default=str)
            logger.info(f"Saved scheduler config with {len(self.jobs_config)} jobs")
        except Exception as e:
            logger.error(f"Error saving scheduler config: {e}")

    def initialize_scheduler(self):
        """Initialize the APScheduler with persistent job store"""
        try:
            # Configure job stores
            jobstores = {
                'default': SQLAlchemyJobStore(
                    url=f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
                )
            }

            # Configure executors
            executors = {
                'default': ThreadPoolExecutor(10)
            }

            # Job defaults
            job_defaults = {
                'coalesce': False,
                'max_instances': 3
            }

            # Create scheduler
            self.scheduler = BackgroundScheduler(
                jobstores=jobstores,
                executors=executors,
                job_defaults=job_defaults,
                timezone='UTC'
            )

            # Start scheduler
            self.scheduler.start()
            logger.info("Scheduler initialized and started")

            # Register shutdown handler
            atexit.register(lambda: self.scheduler.shutdown())

            # Restore jobs from config
            self.restore_jobs()

        except Exception as e:
            logger.error(f"Error initializing scheduler: {e}")
            # Fallback to memory job store
            self.scheduler = BackgroundScheduler()
            self.scheduler.start()
            atexit.register(lambda: self.scheduler.shutdown())

    def restore_jobs(self):
        """Restore jobs from configuration"""
        try:
            for job_config in self.jobs_config:
                self.add_job_from_config(job_config)
            logger.info(f"Restored {len(self.jobs_config)} jobs")
        except Exception as e:
            logger.error(f"Error restoring jobs: {e}")

    def add_job_from_config(self, job_config):
        """Add a job from configuration"""
        try:
            trigger = CronTrigger.from_crontab(job_config['cron_expression'])

            self.scheduler.add_job(
                func=self.run_pipeline_job,
                trigger=trigger,
                id=job_config['id'],
                name=job_config['name'],
                args=[job_config.get('stage', 'full')],
                replace_existing=True
            )

            logger.info(f"Added job: {job_config['name']} ({job_config['id']})")

        except Exception as e:
            logger.error(f"Error adding job from config: {e}")

    def add_schedule(self, schedule_type, cron_expression, stage='full', name=None):
        """Add a new scheduled job"""
        try:
            job_id = f"pipeline_job_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            job_name = name or f"Pipeline {schedule_type} Schedule"

            # Create trigger
            trigger = CronTrigger.from_crontab(cron_expression)

            # Add job to scheduler
            job = self.scheduler.add_job(
                func=self.run_pipeline_job,
                trigger=trigger,
                id=job_id,
                name=job_name,
                args=[stage],
                replace_existing=True
            )

            # Save to config
            job_config = {
                'id': job_id,
                'name': job_name,
                'type': schedule_type,
                'cron_expression': cron_expression,
                'stage': stage,
                'created': datetime.now().isoformat(),
                'next_run': job.next_run_time.isoformat() if job.next_run_time else None
            }

            self.jobs_config.append(job_config)
            self.save_config()

            logger.info(f"Added new schedule: {job_name}")
            return job_config

        except Exception as e:
            logger.error(f"Error adding schedule: {e}")
            return None

    def remove_schedule(self, job_id):
        """Remove a scheduled job"""
        try:
            # Remove from scheduler
            self.scheduler.remove_job(job_id)

            # Remove from config
            self.jobs_config = [job for job in self.jobs_config if job['id'] != job_id]
            self.save_config()

            logger.info(f"Removed schedule: {job_id}")
            return True

        except Exception as e:
            logger.error(f"Error removing schedule: {e}")
            return False

    def clear_all_schedules(self):
        """Clear all scheduled jobs"""
        try:
            # Remove all jobs from scheduler
            for job in self.scheduler.get_jobs():
                if job.id.startswith('pipeline_job_'):
                    self.scheduler.remove_job(job.id)

            # Clear config
            self.jobs_config = []
            self.save_config()

            logger.info("Cleared all schedules")
            return True

        except Exception as e:
            logger.error(f"Error clearing schedules: {e}")
            return False

    def get_active_jobs(self):
        """Get list of active jobs"""
        try:
            active_jobs = []
            for job in self.scheduler.get_jobs():
                if job.id.startswith('pipeline_job_'):
                    job_config = next((j for j in self.jobs_config if j['id'] == job.id), None)
                    if job_config:
                        job_config['next_run'] = job.next_run_time.isoformat() if job.next_run_time else None
                        active_jobs.append(job_config)

            return active_jobs

        except Exception as e:
            logger.error(f"Error getting active jobs: {e}")
            return []

    def run_pipeline_job(self, stage='full'):
        """Execute pipeline job (called by scheduler)"""
        try:
            logger.info(f"Starting scheduled pipeline execution: {stage}")

            # Import here to avoid circular imports
            from etl import run_full_pipeline, build_bronze, build_silver, build_gold

            success = False

            if stage == 'full':
                success = run_full_pipeline()
            elif stage == 'bronze':
                success = build_bronze()
            elif stage == 'silver':
                success = build_silver()
            elif stage == 'gold':
                success = build_gold()

            # Log result
            if success:
                logger.info(f"Scheduled pipeline execution completed successfully: {stage}")
                self.log_execution(stage, 'success')
            else:
                logger.error(f"Scheduled pipeline execution failed: {stage}")
                self.log_execution(stage, 'failed')

            return success

        except Exception as e:
            logger.error(f"Error in scheduled pipeline execution: {e}")
            self.log_execution(stage, 'error', str(e))
            return False

    def log_execution(self, stage, status, error_message=None):
        """Log pipeline execution results"""
        try:
            log_entry = {
                'timestamp': datetime.now().isoformat(),
                'stage': stage,
                'status': status,
                'error_message': error_message
            }

            # Save to execution log file
            log_file = Path(__file__).parent / 'execution_log.json'

            if log_file.exists():
                with open(log_file, 'r') as f:
                    logs = json.load(f)
            else:
                logs = []

            logs.append(log_entry)

            # Keep only last 100 entries
            logs = logs[-100:]

            with open(log_file, 'w') as f:
                json.dump(logs, f, indent=2, default=str)

        except Exception as e:
            logger.error(f"Error logging execution: {e}")

    def get_execution_history(self, limit=10):
        """Get pipeline execution history"""
        try:
            log_file = Path(__file__).parent / 'execution_log.json'

            if log_file.exists():
                with open(log_file, 'r') as f:
                    logs = json.load(f)
                return logs[-limit:] if logs else []
            else:
                return []

        except Exception as e:
            logger.error(f"Error getting execution history: {e}")
            return []

    def is_running(self):
        """Check if scheduler is running"""
        return self.scheduler and self.scheduler.running

    def get_scheduler_info(self):
        """Get scheduler status information"""
        try:
            return {
                'running': self.is_running(),
                'job_count': len(self.scheduler.get_jobs()) if self.scheduler else 0,
                'active_jobs': len([j for j in self.get_active_jobs()]),
                'uptime': str(datetime.now() - datetime.now()) if self.scheduler else None
            }
        except Exception as e:
            logger.error(f"Error getting scheduler info: {e}")
            return {'running': False, 'job_count': 0, 'active_jobs': 0, 'uptime': None}

# Global scheduler instance
_scheduler_manager = None

def get_scheduler_manager():
    """Get or create the global scheduler manager instance"""
    global _scheduler_manager
    if _scheduler_manager is None:
        _scheduler_manager = SchedulerManager()
    return _scheduler_manager

def shutdown_scheduler():
    """Shutdown the global scheduler"""
    global _scheduler_manager
    if _scheduler_manager and _scheduler_manager.scheduler:
        _scheduler_manager.scheduler.shutdown()
        _scheduler_manager = None

if __name__ == "__main__":
    # Test the scheduler manager
    print("Testing Scheduler Manager...")

    manager = get_scheduler_manager()
    print(f"Scheduler running: {manager.is_running()}")
    print(f"Active jobs: {len(manager.get_active_jobs())}")

    # Add a test job (daily at 2 AM)
    test_job = manager.add_schedule("Daily", "0 2 * * *", "full", "Test Daily Pipeline")
    if test_job:
        print(f"Added test job: {test_job['id']}")

    # List active jobs
    jobs = manager.get_active_jobs()
    for job in jobs:
        print(f"Job: {job['name']} - Next run: {job.get('next_run', 'N/A')}")

    print("Scheduler Manager test completed")
