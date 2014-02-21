#
# Copyright 2014 Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import contextlib

from collections import defaultdict

from apache.aurora.client.api import AuroraClientAPI
from apache.aurora.client.api.sla import DomainUpTimeSlaVector
from apache.aurora.client.commands.admin import sla_list_safe_domain
from apache.aurora.client.commands.util import AuroraClientCommandTest
from apache.aurora.common.aurora_job_key import AuroraJobKey

from twitter.common.contextutil import temporary_file

from mock import Mock, patch


class TestAdminSlaListSafeDomainCommand(AuroraClientCommandTest):

  @classmethod
  def setup_mock_options(cls, exclude=None, include=None, override=None, list_jobs=False):
    mock_options = Mock()
    mock_options.exclude_filename = exclude
    mock_options.include_filename = include
    mock_options.override_filename = override
    mock_options.list_jobs = list_jobs
    mock_options.verbosity = False
    return mock_options

  @classmethod
  def create_hosts(cls, num_hosts, percentage, duration):
    hosts = defaultdict(list)
    for i in range(num_hosts):
      host_name = 'h%s' % i
      job = AuroraJobKey.from_path('west/role/env/job%s' % i)
      hosts[host_name].append(DomainUpTimeSlaVector.JobUpTimeLimit(job, percentage, duration))
    return hosts

  @classmethod
  def create_mock_vector(cls, result):
    mock_vector = Mock(spec=DomainUpTimeSlaVector)
    mock_vector.get_safe_hosts.return_value = result
    return mock_vector

  def test_safe_domain_no_options(self):
    """Tests successful execution of the sla_list_safe_domain command without extra options."""
    mock_options = self.setup_mock_options()
    mock_vector = self.create_mock_vector(self.create_hosts(3, 80, 100))
    with contextlib.nested(
        patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
        patch('apache.aurora.client.commands.admin.print_results'),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)
    ) as (
        mock_api,
        mock_print_results,
        test_clusters,
        mock_options):

      mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector
      sla_list_safe_domain(['west', '50', '100s'])

      mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
      mock_print_results.assert_called_once_with(['h0', 'h1', 'h2'])

  def test_safe_domain_exclude_hosts(self):
    """Test successful execution of the sla_list_safe_domain command with exclude hosts option."""
    mock_vector = self.create_mock_vector(self.create_hosts(3, 80, 100))
    with temporary_file() as fp:
      fp.write('h1')
      fp.flush()
      mock_options = self.setup_mock_options(exclude=fp.name)
      with contextlib.nested(
          patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
          patch('apache.aurora.client.commands.admin.print_results'),
          patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('twitter.common.app.get_options', return_value=mock_options)
      ) as (
          mock_api,
          mock_print_results,
          test_clusters,
          mock_options):

        mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector

        sla_list_safe_domain(['west', '50', '100s'])

        mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
        mock_print_results.assert_called_once_with(['h0', 'h2'])

  def test_safe_domain_include_hosts(self):
    """Test successful execution of the sla_list_safe_domain command with include hosts option."""
    mock_vector = self.create_mock_vector(self.create_hosts(3, 80, 100))
    with temporary_file() as fp:
      fp.write('h1')
      fp.flush()
      mock_options = self.setup_mock_options(include=fp.name)
      with contextlib.nested(
          patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
          patch('apache.aurora.client.commands.admin.print_results'),
          patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('twitter.common.app.get_options', return_value=mock_options)
      ) as (
          mock_api,
          mock_print_results,
          test_clusters,
          mock_options):

        mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector

        sla_list_safe_domain(['west', '50', '100s'])

        mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
        mock_print_results.assert_called_once_with(['h1'])

  def test_safe_domain_override_jobs(self):
    """Test successful execution of the sla_list_safe_domain command with override_jobs option."""
    mock_vector = self.create_mock_vector(self.create_hosts(3, 80, 100))
    with temporary_file() as fp:
      fp.write('west/role/env/job1 30 200s')
      fp.flush()
      mock_options = self.setup_mock_options(override=fp.name)
      with contextlib.nested(
          patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
          patch('apache.aurora.client.commands.admin.print_results'),
          patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
          patch('twitter.common.app.get_options', return_value=mock_options)
      ) as (
          mock_api,
          mock_print_results,
          test_clusters,
          mock_options):

        mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector

        sla_list_safe_domain(['west', '50', '100s'])

        job_key = AuroraJobKey.from_path('west/role/env/job1')
        override = {job_key: DomainUpTimeSlaVector.JobUpTimeLimit(job_key, 30, 200)}
        mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, override)
        mock_print_results.assert_called_once_with(['h0', 'h1', 'h2'])

  def test_safe_domain_list_jobs(self):
    """Tests successful execution of the sla_list_safe_domain command with list_jobs option."""
    mock_options = self.setup_mock_options(list_jobs=True)
    mock_vector = self.create_mock_vector(self.create_hosts(3, 50, 100))
    with contextlib.nested(
        patch('apache.aurora.client.commands.admin.AuroraClientAPI', new=Mock(spec=AuroraClientAPI)),
        patch('apache.aurora.client.commands.admin.print_results'),
        patch('apache.aurora.client.commands.admin.CLUSTERS', new=self.TEST_CLUSTERS),
        patch('twitter.common.app.get_options', return_value=mock_options)
    ) as (
        mock_api,
        mock_print_results,
        test_clusters,
        mock_options):

      mock_api.return_value.sla_get_safe_domain_vector.return_value = mock_vector
      sla_list_safe_domain(['west', '50', '100s'])

      mock_vector.get_safe_hosts.assert_called_once_with(50.0, 100.0, {})
      mock_print_results.assert_called_once_with([
          'h0 west/role/env/job0 50.00 100',
          'h1 west/role/env/job1 50.00 100',
          'h2 west/role/env/job2 50.00 100'])

  def test_safe_domain_invalid_percentage(self):
    """Tests execution of the sla_list_safe_domain command with invalid percentage"""
    mock_options = self.setup_mock_options()
    with patch('twitter.common.app.get_options', return_value=mock_options) as (mock_options):

      try:
        sla_list_safe_domain(['west', '0', '100s'])
      except SystemExit:
        pass
      else:
        assert 'Expected error is not raised.'

  def test_safe_domain_malformed_job_override(self):
    """Tests execution of the sla_list_safe_domain command with invalid job_override file"""
    with temporary_file() as fp:
      fp.write('30 200s')
      fp.flush()
      mock_options = self.setup_mock_options(override=fp.name)
      with patch('twitter.common.app.get_options', return_value=mock_options) as (mock_options):

        try:
          sla_list_safe_domain(['west', '50', '100s'])
        except SystemExit:
          pass
        else:
          assert 'Expected error is not raised.'
