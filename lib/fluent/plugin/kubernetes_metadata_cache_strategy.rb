# frozen_string_literal: true

#
# Fluentd Kubernetes Metadata Filter Plugin - Enrich Fluentd events with
# Kubernetes metadata
#
# Copyright 2017 Red Hat, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

require 'fluent/clock'

module KubernetesMetadata
  module CacheStrategy
    def get_pod_metadata(key, namespace_name, pod_name, time, batch_miss_cache, key_is_pod_id: false) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/MethodLength, Metrics/ParameterLists, Metrics/PerceivedComplexity
      metadata = {}
      ids = @id_cache[key]
      if ids.nil?
        @stats.bump(:id_cache_miss)
        if batch_miss_cache.key?("#{namespace_name}_#{pod_name}")
          return batch_miss_cache["#{namespace_name}_#{pod_name}"]
        end

        pod_metadata = fetch_pod_metadata_unless_failed(key, namespace_name, pod_name)
        if @skip_namespace_metadata
          ids = { pod_id: pod_metadata['pod_id'] }
          if ids[:pod_id].nil?
            # the lookup failed: a pod UID key stands in for the pod, and a
            # container ID key waits for the slow path to set the UID
            ids[:pod_id] = key if key_is_pod_id
            ids[:pod_id_unknown] = true unless key_is_pod_id
          end
          @id_cache[key] = ids
          return pod_metadata
        end

        namespace_metadata = fetch_namespace_metadata(namespace_name)
        ids = { pod_id: pod_metadata['pod_id'], namespace_id: namespace_metadata['namespace_id'] }
        if !ids[:pod_id].nil? && !ids[:namespace_id].nil?
          # pod found and namespace found
          metadata = pod_metadata.merge(namespace_metadata)
        else
          if ids[:pod_id].nil? && !ids[:namespace_id].nil? # rubocop:disable Style/IfInsideElse
            # pod not found, but namespace found
            @stats.bump(:id_cache_pod_not_found_namespace)
            ns_time = Time.parse(namespace_metadata['creation_timestamp'])
            if ns_time <= Time.at(time.to_f) # rubocop:disable Metrics/BlockNesting
              # namespace is older then record for pod
              ids[:pod_id] = key
              # the key stands in for the pod UID, which is still unknown
              # when it is a container ID
              ids[:pod_id_unknown] = true unless key_is_pod_id # rubocop:disable Metrics/BlockNesting
              metadata = @cache.fetch(ids[:pod_id]) do
                { 'pod_id' => ids[:pod_id] }
              end
            end
            metadata = metadata.merge(namespace_metadata)
          else
            if !ids[:pod_id].nil? && ids[:namespace_id].nil? # rubocop:disable Metrics/BlockNesting
              # pod found, but namespace NOT found
              # this should NEVER be possible since pod meta can
              # only be retrieved with a namespace
              @stats.bump(:id_cache_namespace_not_found_pod)
            else
              # nothing found
              @stats.bump(:id_cache_orphaned_record)
            end
            if @allow_orphans # rubocop:disable Metrics/BlockNesting
              log.trace("orphaning message for: #{namespace_name}/#{pod_name} ")
              metadata = {
                'orphaned_namespace' => namespace_name,
                'namespace_name' => @orphaned_namespace_name,
                'namespace_id' => @orphaned_namespace_id
              }
            else
              metadata = {}
            end
            batch_miss_cache["#{namespace_name}_#{pod_name}"] = metadata
          end
        end
        @id_cache[key] = ids unless batch_miss_cache.key?("#{namespace_name}_#{pod_name}")
      else
        # SLOW PATH
        metadata = @cache.fetch(ids[:pod_id]) do
          @stats.bump(:pod_cache_miss)
          m = fetch_pod_metadata_unless_failed(key, namespace_name, pod_name)
          m.nil? || m.empty? ? { 'pod_id' => ids[:pod_id] } : m
        end
        if ids[:pod_id_unknown] && metadata['pod_id'] && metadata['pod_id'] != ids[:pod_id]
          # fetch_pod_metadata caches the pod under its UID. Point the
          # container to it, so later records hit the cache.
          @id_cache[key] = ids.except(:pod_id_unknown).merge(pod_id: metadata['pod_id'])
        end
        namespace_metadata = @namespace_cache.fetch(ids[:namespace_id]) do
          m = unless @skip_namespace_metadata
                @stats.bump(:namespace_cache_miss)
                fetch_namespace_metadata(namespace_name)
              end
          m.nil? || m.empty? ? { 'namespace_id' => ids[:namespace_id] } : m
        end
        metadata = metadata.merge(namespace_metadata)
      end

      # remove namespace info that is only used for comparison
      metadata.delete('creation_timestamp')
      metadata.delete_if { |_k, v| v.nil? }
    end

    private

    # The key is the one of the log, not the pod name, so a new pod with the
    # same name is not held back by a failure for the old one.
    def fetch_pod_metadata_unless_failed(key, namespace_name, pod_name)
      return fetch_pod_metadata(namespace_name, pod_name) unless remember_failed_lookup?(key)

      return {} if @failed_pod_lookups.include?(key)

      metadata = fetch_pod_metadata(namespace_name, pod_name)
      @failed_pod_lookups.add(key) if metadata.nil? || metadata.empty?
      metadata
    end

    # Logs of different pods can share a key that is not a string or is blank.
    # String#b keeps strip from raising on an invalid byte sequence.
    def remember_failed_lookup?(key)
      @failed_pod_lookups && key.is_a?(String) && !key.b.strip.empty?
    end
  end

  # Remembers the keys whose pod lookup failed until the retry interval passes.
  # The keys stay in the order they failed, so the oldest ones expire and are
  # evicted first. Fluent::Clock is monotonic, so a wall clock step does not
  # change the wait.
  class FailedLookups
    def initialize(max_size, interval)
      @max_size = max_size
      @interval = interval
      @retry_at = {}
      @lock = Mutex.new
    end

    def include?(key)
      @lock.synchronize do
        evict_expired
        @retry_at.key?(key)
      end
    end

    def add(key)
      @lock.synchronize do
        @retry_at.delete(key)
        @retry_at[key] = Fluent::Clock.now + @interval
        evict_expired
        @retry_at.shift while @retry_at.size > @max_size
      end
    end

    private

    def evict_expired
      now = Fluent::Clock.now
      @retry_at.shift while !@retry_at.empty? && @retry_at.first[1] <= now
    end
  end
end
