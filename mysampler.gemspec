# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'mysampler/version'

Gem::Specification.new do |spec|
  spec.name          = "mysampler"
  spec.version       = MySampler::VERSION
  spec.authors       = ["Aaron Brown"]
  spec.email         = ["aaron@9minutesnooze.com"]
  spec.summary       = %q{A utility that logs MySQL statistics to CSV or graphite.}
  spec.description   = %q{MySampler is a tool written in ruby to poll SHOW GLOBAL STATUS in MySQL and output the values to either a CSV or graphite/carbon. The interval at which the polling occurs can be specified and the output can be either the absolute or relative values, so you can see change over time. If logging to CSV, the a date stamp is appended to the CSV file and it is rotated hourly (to be configurable later).}
  spec.homepage      = "https://github.com/9minutesnooze/mysampler"
  spec.license       = "MIT"

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler", "~> 1.5"
  spec.add_development_dependency "rake"
  %w{mysql sequel graphite}.each { |gem| spec.add_dependency gem }
end
