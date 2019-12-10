Gem::Specification.new do |spec|
  spec.name          = 'h4-ruby-druid'
  spec.version       = '0.1.15'
  spec.authors       = ['LiquidM, Inc.']
  spec.email         = ['opensource@liquidm.com']
  spec.summary       = 'Ruby client for metamx druid'
  spec.description   = 'Ruby client for metamx druid'
  spec.homepage      = 'https://github.com/TetrationAnalytics/ruby-druid'
  spec.license       = 'MIT'

  spec.files         = `git ls-files`.split($/)
  spec.executables   = spec.files.grep(%r{^bin/}) { |f| File.basename(f) }
  spec.test_files    = spec.files.grep(%r{^(test|spec|features)/})
  spec.require_paths = ['lib']

  spec.add_dependency 'rest-client'
end
