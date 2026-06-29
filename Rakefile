# frozen_string_literal: true

require 'bundler/setup'
require 'bundler/gem_tasks'
require 'rake/testtask'
require 'bump/tasks'
require 'rubocop/rake_task'

task test: [:base_test]
task default: [:test, :build, :rubocop]

RuboCop::RakeTask.new

desc 'Run test_unit based test'
Rake::TestTask.new(:base_test) do |task|
  # To run test for only one file (or file path pattern)
  #  $ bundle exec rake base_test TEST=test/test_specified_path.rb
  #  $ bundle exec rake base_test TEST=test/test_*.rb
  task.pattern = 'test/**/test_*.rb'
  task.warning = false
end
