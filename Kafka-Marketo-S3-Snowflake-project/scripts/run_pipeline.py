#!/usr/bin/env python3
"""
Pipeline orchestrator script to run all components of the Marketo to Snowflake ETL pipeline.
This script manages the lifecycle of all pipeline components.
"""

import os
import sys
import subprocess
import time
import signal
import threading
from typing import List, Dict
import argparse

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.logger import setup_logger

class PipelineOrchestrator:
    def __init__(self):
        self.logger = setup_logger("pipeline_orchestrator")
        self.processes: Dict[str, subprocess.Popen] = {}
        self.should_stop = False
        
        # Pipeline components configuration
        self.components = {
            'producer': {
                'script': 'producers/marketo_producer.py',
                'description': 'Marketo Data Producer',
                'startup_delay': 0
            },
            'stream_processor': {
                'script': 'processors/stream_processor.py', 
                'description': 'Stream Processor',
                'startup_delay': 5
            },
            'data_lake_sink': {
                'script': 'processors/data_lake_sink.py',
                'description': 'Data Lake Sink',
                'startup_delay': 10
            },
            'snowflake_consumer': {
                'script': 'consumers/snowflake_consumer.py',
                'description': 'Snowflake Consumer', 
                'startup_delay': 15
            }
        }
    
    def signal_handler(self, signum, frame):
        self.logger.info("Received shutdown signal, stopping pipeline...")
        self.should_stop = True
        self.stop_all_components()
    
    def start_component(self, component_name: str, config: Dict) -> bool:
        try:
            script_path = os.path.join(os.path.dirname(__file__), config['script'])
            
            if not os.path.exists(script_path):
                self.logger.error(f"Script not found: {script_path}")
                return False
            
            self.logger.info(f"Starting {config['description']}...")
            
            process = subprocess.Popen(
                [sys.executable, script_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                bufsize=1
            )
            
            self.processes[component_name] = process
            
            # Start log monitoring thread
            log_thread = threading.Thread(
                target=self.monitor_component_logs,
                args=(component_name, process),
                daemon=True
            )
            log_thread.start()
            
            self.logger.info(f"{config['description']} started with PID {process.pid}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to start {component_name}: {e}")
            return False
    
    def monitor_component_logs(self, component_name: str, process: subprocess.Popen):
        try:
            while process.poll() is None and not self.should_stop:
                line = process.stdout.readline()
                if line:
                    self.logger.info(f"[{component_name}] {line.strip()}")
                
                error_line = process.stderr.readline()
                if error_line:
                    self.logger.error(f"[{component_name}] {error_line.strip()}")
                    
        except Exception as e:
            self.logger.error(f"Error monitoring logs for {component_name}: {e}")
    
    def check_component_health(self, component_name: str) -> bool:
        if component_name not in self.processes:
            return False
        
        process = self.processes[component_name]
        return process.poll() is None
    
    def restart_component(self, component_name: str) -> bool:
        self.logger.info(f"Restarting component: {component_name}")
        
        # Stop the component if running
        if component_name in self.processes:
            self.stop_component(component_name)
        
        # Wait a moment before restarting
        time.sleep(2)
        
        # Start the component again
        config = self.components[component_name]
        return self.start_component(component_name, config)
    
    def stop_component(self, component_name: str) -> bool:
        if component_name not in self.processes:
            return True
        
        try:
            process = self.processes[component_name]
            self.logger.info(f"Stopping {self.components[component_name]['description']}...")
            
            process.terminate()
            
            # Wait for graceful shutdown
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                self.logger.warning(f"Component {component_name} didn't stop gracefully, forcing kill")
                process.kill()
                process.wait()
            
            del self.processes[component_name]
            self.logger.info(f"Component {component_name} stopped")
            return True
            
        except Exception as e:
            self.logger.error(f"Error stopping component {component_name}: {e}")
            return False
    
    def stop_all_components(self):
        self.logger.info("Stopping all pipeline components...")
        
        # Stop in reverse order for graceful shutdown
        component_order = list(self.components.keys())
        for component_name in reversed(component_order):
            self.stop_component(component_name)
    
    def health_check_loop(self):
        self.logger.info("Starting health check monitoring...")
        
        while not self.should_stop:
            unhealthy_components = []
            
            for component_name in self.components.keys():
                if not self.check_component_health(component_name):
                    unhealthy_components.append(component_name)
            
            if unhealthy_components:
                self.logger.warning(f"Unhealthy components detected: {unhealthy_components}")
                
                # Restart unhealthy components
                for component_name in unhealthy_components:
                    if not self.should_stop:
                        self.restart_component(component_name)
            
            # Health check interval
            time.sleep(30)
    
    def run_pipeline(self, components: List[str] = None):
        if components is None:
            components = list(self.components.keys())
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.logger.info("Starting Marketo to Snowflake ETL Pipeline...")
        self.logger.info(f"Components to run: {components}")
        
        try:
            # Start components with staggered delays
            for component_name in components:
                if self.should_stop:
                    break
                
                config = self.components[component_name]
                
                # Wait for startup delay
                if config['startup_delay'] > 0:
                    self.logger.info(f"Waiting {config['startup_delay']} seconds before starting {component_name}...")
                    time.sleep(config['startup_delay'])
                
                success = self.start_component(component_name, config)
                if not success:
                    self.logger.error(f"Failed to start {component_name}, aborting pipeline start")
                    self.stop_all_components()
                    return False
            
            # Start health monitoring
            health_thread = threading.Thread(target=self.health_check_loop, daemon=True)
            health_thread.start()
            
            self.logger.info("All components started successfully!")
            self.logger.info("Pipeline is running. Press Ctrl+C to stop.")
            
            # Main loop - wait for shutdown signal
            while not self.should_stop:
                time.sleep(1)
            
            return True
            
        except KeyboardInterrupt:
            self.logger.info("Received interrupt signal")
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
        finally:
            self.stop_all_components()
            self.logger.info("Pipeline stopped")
        
        return False

def main():
    parser = argparse.ArgumentParser(description='Run Marketo to Snowflake ETL Pipeline')
    parser.add_argument(
        '--components', 
        nargs='+',
        choices=['producer', 'stream_processor', 'data_lake_sink', 'snowflake_consumer'],
        help='Specific components to run (default: all)'
    )
    parser.add_argument(
        '--list-components',
        action='store_true',
        help='List all available components'
    )
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator()
    
    if args.list_components:
        print("Available pipeline components:")
        for name, config in orchestrator.components.items():
            print(f"  {name}: {config['description']}")
        return
    
    success = orchestrator.run_pipeline(components=args.components)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()