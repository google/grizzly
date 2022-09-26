# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Module for running CB triggers during install process.

   Is invoked by apply_grizzly_terraform.sh

   Typical usage example:
   python3 ./run_build_triggers.py project-id branch-name
"""
import sys
import threading
import time
from typing import Dict, Any, List

from google.cloud.devtools import cloudbuild_v1


client = cloudbuild_v1.CloudBuildClient()


class Trigger(threading.Thread):
  """Represents Cloud Build trigger configuration.

  Configures, runs, and monitors CB trigger run.

  Attributes:
    gcp_project (str): GCP project id.
    branch (str): branch name to run trigger on.
    trigger_name (str): name of the trigger to run.
    substitutions (Dict[str, Any], default - None): dictionary of substitutions
      for trigger. Keys are the names of substituted variables,
      and variables are substitutions.

    _request (RunBuildTriggerRequest): request that will be passed to a client
    _failed (bool): whether the run has failed.
    _cancelled (bool): whether the run was cancelled.
    _operation (cloudbuild_v1.operation.Operation): operation object returned
      by CB client. Contains the status and, eventually, the result.
  """

  def __init__(self, gcp_project: str,
               branch: str, trigger_name: str,
               substitutions: Dict[str, Any] = None) -> None:
    """Initialize trigger."""
    self.gcp_project = gcp_project
    self.branch = branch
    self.trigger_name = trigger_name
    self.substitutions = substitutions

    # construct request
    full_trigger_name = 'projects/{}/locations/global/triggers/{}'.format(
        self.gcp_project, self.trigger_name)

    self._request = cloudbuild_v1.RunBuildTriggerRequest({
        'name': full_trigger_name,
        'project_id': self.gcp_project,
        'source': {
            'project_id': self.gcp_project,
            'branch_name': self.branch,
            'substitutions': self.substitutions,
        }
    })
    # initialize parent class
    threading.Thread.__init__(self, target=self.run_trigger, name=str(self))
    self._failed = False
    self._cancelled = False
    self._operation = None

  def run_trigger(self) -> None:
    """Runs and monitors the status of CB trigger."""
    try:
      # submit the request
      self._operation = client.run_build_trigger(request=self._request)
      # loop while the operation is not completed
      while not self._operation.done():
        # stop if cancellation request is received
        if self._cancelled:
          break
        time.sleep(1)
      # evaluate the result to check for errors
      self._operation.result()
    except Exception as e:
      # in case of any exception, cancel the request
      # and indicate the run as failed
      self.cancel()
      self._failed = True
      raise e

  @property
  def failed(self):
    """Function to make failed property read-only."""
    return self._failed

  def cancel(self):
    """Cancel the request."""
    print('Cancelling', self)
    # mark self as cancelled to stop monitoring loop
    self._cancelled = True
    if self._operation is not None:
      # cancel the request if it was submitted
      self._operation.cancel()

  def __str__(self):
    """Formatting for printing."""
    return 'Trigger {} on branch {} with substitutions {}'.format(
        self.trigger_name, self.branch, self.substitutions)


def cancel_triggers(triggers: List[Trigger], message: str) -> None:
  """Function to cancel all triggers and exit.

  Args:
    triggers (List[Trigger]): list of triggers that are still active.
    message (str): the message to display in the log.
  """
  print(f'Error occurred while {message}')
  # stop all triggers and join the threads
  for trigger in triggers:
    trigger.cancel()
    trigger.join()
  exit(-1)


def run_triggers_parallel(triggers: List[Trigger], message: str):
  """Function to run a list of triggers simultaneously.

  Args:
    triggers (List[Trigger]): list of triggers to run.
    message (str): the message to display in the log.
  """
  # start all threads
  for trigger in triggers:
    trigger.start()

  print(f'Started {message}')
  start_time = time.time()
  last_update_time = start_time
  all_done = False

  while not all_done:
    all_done = True
    # check the triggers statuses
    for trigger in triggers:
      # if the trigger monitoring finished running
      if not trigger.is_alive():
        trigger.join()
        # remove trigger from the active list
        triggers.remove(trigger)
        # in case of failure stop all triggers
        if trigger.failed:
          cancel_triggers(triggers, message)
      else:
        all_done = False

    time.sleep(1)
    current_time = time.time()
    time_elapsed = int(current_time - start_time)
    # provide an update every 10 seconds
    if current_time - last_update_time >= 10:
      print(f'{message[0].upper() + message[1:]} [{time_elapsed}s elapsed]')
      last_update_time = current_time

  print(f'Finished {message}')


def main() -> None:
  """Main function with trigger configuration."""
  # commandline args
  gcp_project = sys.argv[1]
  branch = sys.argv[2]

  # first trigger pool: BigQuery
  bq_trigger = [Trigger(gcp_project, branch, 'deploy-bigquery', substitutions={
      '_DOMAIN': 'bq', '_ENVIRONMENT': branch, '_SCOPE_FILE': 'SCOPE'
    })]

  # second trigger pool: Deploy Triggers
  deploy_triggers = [
      Trigger(gcp_project, branch, 'deploy-datacatalog'),
      Trigger(gcp_project, branch, 'deploy-dlp-deidentify'),
      Trigger(gcp_project, branch, 'deploy-dlp-inspect'),
      Trigger(gcp_project, branch, 'deploy-policytag-taxonomy'),
  ]

  # third trigger pool: Import Triggers
  import_triggers = [
      Trigger(gcp_project, branch, 'import-taxonomy'),
      Trigger(gcp_project, branch, 'import-git-rep'),
  ]

  # names of example domains to deploy
  domains = [
      'base/bas_austin_crime',
      'base/bas_austin_crime_with_date',
      'base/bas_census_bureau_acs',
      'base/bas_chicago_crime',
      'base/bas_gdelt',
      'base/bas_geo_openstreetmap',
      'base/bas_geo_us_boundaries',
      'geo',
      'store_research',
      'metadata',
  ]
  # generate fourth trigger pool: Composer Deploy triggers
  composer_triggers = []
  for domain in domains:
    composer_triggers.append(Trigger(gcp_project, branch, 'deploy-composer',
                                     substitutions={'_DOMAIN': domain,
                                                    '_ENVIRONMENT': branch}))

  run_triggers_parallel(bq_trigger, message='deploying BigQuery')
  run_triggers_parallel(deploy_triggers, message='running deploy triggers')
  run_triggers_parallel(import_triggers, message='running import triggers')
  # fourth pool can't be run in parallel, so run it sequentially
  for trigger in composer_triggers:
    domain = trigger.substitutions['_DOMAIN']
    run_triggers_parallel([trigger], message=f'deploying {domain}')


if __name__ == '__main__':
  main()
