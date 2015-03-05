require 'multi_json'

module Druid
  class DataSource

    attr_reader :name, :uri, :metrics, :dimensions

    def initialize(name, uri)
      @name = name.split('/').last
      if uri.is_a?(String)
        @uri = URI(uri)
      else
        @uri = uri
      end
    end

    def metadata
      @metadata ||= metadata!
    end

    def metadata!
      meta_path = "#{@uri.path}datasources/#{name}"
      req = Net::HTTP::Get.new(meta_path)
      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = 60_000 # ms
        http.request(req)
      end

      if response.code != '200'
        raise "Request failed: #{response.code}: #{response.body}"
      end

      MultiJson.load(response.body)
    end

    def metrics
      @metrics ||= metadata['metrics']
    end

    def dimensions
      @dimensions ||= metadata['dimensions']
    end

    def query(query)
      query.dataSource = name

      req = Net::HTTP::Post.new(uri.path, { 'Content-Type' => 'application/json' })
      req.body = query.to_json

      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = 60_000 # ms
        http.request(req)
      end

      if response.code != '200'
        # ignore GroupBy cache issues and try againg without cached results
        if query.context.useCache != false && response.code == "500" && response.body =~ /Cannot have a null result!/
          query.context.useCache = false
          return self.query(query)
        end

        raise "Request failed: #{response.code}: #{response.body}"
      end

      MultiJson.load(response.body)
    end

  end
end