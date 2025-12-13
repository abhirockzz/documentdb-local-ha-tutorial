#!/usr/bin/env python3
from pymongo import MongoClient
from pymongo.errors import (
    ConnectionFailure,
    AutoReconnect,
    ServerSelectionTimeoutError,
    OperationFailure,
    NotPrimaryError,
)
import time
from datetime import datetime
import sys


class ReadWriteFailoverTest:
    def __init__(
        self,
        host="localhost",
        port=10260,
        username="k8s_secret_user",
        password="K8sSecret100",
    ):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.client = None
        self.db = None
        self.collection = None

        # Metrics
        self.write_count = 0
        self.read_count = 0
        self.write_failures = 0
        self.read_failures = 0
        self.failover_detected = False
        self.failover_start_time = None
        self.write_latencies = []
        self.read_latencies = []

    def log(self, msg, level="INFO"):
        """Timestamped logging"""
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        prefix = {
            "INFO": "ℹ",
            "SUCCESS": "✓",
            "ERROR": "✗",
            "WARN": "⚠",
            "RECOVERY": "↻",
        }.get(level, "•")
        print(f"[CLIENT][{timestamp}] {prefix} {msg}", flush=True)

    def connect(self):
        """Establish DocumentDB connection"""
        try:
            self.client = MongoClient(
                f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/?authSource=admin&authMechanism=SCRAM-SHA-256&tls=true&tlsAllowInvalidCertificates=true",
                serverSelectionTimeoutMS=5000,
                socketTimeoutMS=10000,
                connectTimeoutMS=5000,
                retryWrites=True,
                retryReads=True,
                maxPoolSize=10,
            )

            # Test connection
            self.client.admin.command("ping")
            self.db = self.client.testdb
            self.collection = self.db.failover_test

            self.log(f"Connected to DocumentDB", "SUCCESS")
            return True

        except Exception as e:
            self.log(f"Connection failed: {e}", "ERROR")
            return False

    def reconnect_with_backoff(self):
        """Attempt to reconnect with exponential backoff"""
        backoff = 1.0
        max_backoff = 10.0

        while True:
            try:
                # Close old client
                if self.client:
                    try:
                        self.client.close()
                    except:
                        pass

                # Try to reconnect
                self.log(
                    f"Attempting reconnection (backoff: {backoff:.1f}s)...", "RECOVERY"
                )
                if self.connect():
                    self.log("Reconnection successful!", "SUCCESS")
                    return True

            except Exception as e:
                self.log(f"Reconnection failed: {e}", "WARN")

            time.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

    def perform_write_operation(self):
        """Perform a write operation and measure latency"""
        start_time = time.time()

        try:
            doc = {
                "operation_id": self.write_count,
                "timestamp": datetime.now(),
                "test_run": "failover_test",
                "type": "write",
            }

            result = self.collection.insert_one(doc)
            latency = (time.time() - start_time) * 1000  # Convert to ms

            return True, latency, result.inserted_id

        except (
            ConnectionFailure,
            AutoReconnect,
            ServerSelectionTimeoutError,
            NotPrimaryError,
            OperationFailure,
        ) as e:
            latency = (time.time() - start_time) * 1000
            return False, latency, str(e)

    def perform_read_operation(self):
        """Perform a read operation and measure latency"""
        start_time = time.time()

        try:
            count = self.collection.count_documents({})
            latency = (time.time() - start_time) * 1000  # Convert to ms

            return True, latency, count

        except (
            ConnectionFailure,
            AutoReconnect,
            ServerSelectionTimeoutError,
            OperationFailure,
        ) as e:
            latency = (time.time() - start_time) * 1000
            return False, latency, str(e)

    def handle_successful_write(self, latency):
        """Handle successful write operation"""
        self.write_count += 1
        self.write_latencies.append(latency)

    def handle_successful_read(self, latency, count):
        """Handle successful read operation"""
        self.read_count += 1
        self.read_latencies.append(latency)

    def handle_failed_write(self, latency, error):
        """Handle failed write operation"""
        self.write_failures += 1

        # First failure - failover detected
        if not self.failover_detected:
            self.failover_detected = True
            self.failover_start_time = time.time()

            self.log("=" * 80)
            self.log("FAILOVER EVENT DETECTED", "ERROR")
            self.log(f"  Error: {error}")
            self.log(f"  Last successful write: #{self.write_count}")
            self.log("=" * 80)

    def handle_failed_read(self, latency, error):
        """Handle failed read operation"""
        self.read_failures += 1

    def check_recovery(self):
        """Check if we've recovered from failover"""
        if self.failover_detected and self.write_failures > 0:
            downtime = time.time() - self.failover_start_time
            self.log("=" * 80)
            self.log(f"RECOVERY COMPLETE", "RECOVERY")
            self.log(f"  Total Downtime: {downtime:.2f} seconds")
            self.log(f"  Write Impact:")
            self.log(f"    Failed writes: {self.write_failures}")
            self.log(f"    Successful writes: {self.write_count}")
            self.log(f"  Read Impact:")
            self.log(f"    Failed reads: {self.read_failures}")
            self.log(f"    Successful reads: {self.read_count}")

            if self.read_failures == 0:
                self.log(f"  ✓ Reads continued serving during failover!", "SUCCESS")
            else:
                self.log(f"  ⚠ Some reads also failed during failover", "WARN")

            self.log("=" * 80)

            # Reset failover tracking
            self.failover_detected = False
            self.failover_start_time = None
            self.write_failures = 0
            self.read_failures = 0

    def run_test(self, interval=0.5):
        """Main test loop - continuously perform read and write operations"""
        self.log("=" * 80)
        self.log("DocumentDB Read/Write Failover Test Started")
        self.log("=" * 80)
        self.log(f"Target: {self.host}:{self.port}")
        self.log(f"Operation interval: {interval}s")
        self.log("")
        self.log("Instructions:")
        self.log("  1. Let this run for a few seconds to establish baseline")
        self.log("  2. Trigger failover (delete primary pod)")
        self.log("  3. Observe differential impact on reads vs writes")
        self.log("  4. Press Ctrl+C to stop")
        self.log("=" * 80)
        self.log("")

        if not self.connect():
            self.log("Failed to connect. Exiting.", "ERROR")
            return

        try:
            while True:
                # Perform write operation
                write_success, write_latency, write_result = (
                    self.perform_write_operation()
                )

                if write_success:
                    self.handle_successful_write(write_latency)

                    # Check for recovery
                    if self.failover_detected:
                        self.check_recovery()

                    # Perform read operation
                    read_success, read_latency, read_result = (
                        self.perform_read_operation()
                    )

                    if read_success:
                        self.handle_successful_read(read_latency, read_result)

                        # Show normal operation
                        avg_write_latency = sum(self.write_latencies[-10:]) / min(
                            len(self.write_latencies), 10
                        )
                        avg_read_latency = sum(self.read_latencies[-10:]) / min(
                            len(self.read_latencies), 10
                        )
                        self.log(
                            f"W#{self.write_count} ({write_latency:.0f}ms) | R#{self.read_count} ({read_latency:.0f}ms) | Avg W:{avg_write_latency:.0f}ms R:{avg_read_latency:.0f}ms | Docs: {read_result}",
                            "SUCCESS",
                        )
                    else:
                        self.handle_failed_read(read_latency, read_result)
                        self.log(
                            f"Write OK, Read FAILED: {read_result}",
                            "WARN",
                        )

                    time.sleep(interval)
                else:
                    self.handle_failed_write(write_latency, write_result)

                    # Try read operation even though write failed
                    read_success, read_latency, read_result = (
                        self.perform_read_operation()
                    )

                    if read_success:
                        self.handle_successful_read(read_latency, read_result)
                        downtime = time.time() - self.failover_start_time
                        self.log(
                            f"⚠ Write FAILED | ✓ Read OK ({read_latency:.0f}ms) | Downtime: {downtime:.1f}s | Failed writes: {self.write_failures}",
                            "WARN",
                        )
                    else:
                        self.handle_failed_read(read_latency, read_result)
                        downtime = time.time() - self.failover_start_time
                        self.log(
                            f"⚠ Write FAILED | ⚠ Read FAILED | Downtime: {downtime:.1f}s | Failed W: {self.write_failures} R: {self.read_failures}",
                            "ERROR",
                        )

                    # Attempt to reconnect when operations fail
                    self.reconnect_with_backoff()

        except KeyboardInterrupt:
            self.log("")
            self.log("=" * 80)
            self.log("Test stopped by user")
            self.log(f"Total successful writes: {self.write_count}")
            self.log(f"Total successful reads: {self.read_count}")
            if self.write_latencies:
                avg_w = sum(self.write_latencies) / len(self.write_latencies)
                self.log(f"Average write latency: {avg_w:.0f}ms")
            if self.read_latencies:
                avg_r = sum(self.read_latencies) / len(self.read_latencies)
                self.log(f"Average read latency: {avg_r:.0f}ms")
            self.log("=" * 80)
        finally:
            if self.client:
                self.client.close()


if __name__ == "__main__":
    # Configuration
    HOST = "127.0.0.1"
    PORT = 10260
    USERNAME = "k8s_secret_user"
    PASSWORD = "K8sSecret100"

    # Create and run test client
    test_client = ReadWriteFailoverTest(
        host=HOST, port=PORT, username=USERNAME, password=PASSWORD
    )

    test_client.run_test(interval=0.5)
