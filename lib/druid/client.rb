require 'request_store'

module Druid
  class Client
    X_REQUEST_ID = 'X-Request-Id'.freeze
    X_ILB_REQUEST_ID = 'X-ILB-Request-Id'.freeze

    def initialize(broker_url, opts = nil)
      opts ||= {}
      @broker_url = broker_url
      raise "Invalid broker url: #{broker_url}" unless broker_uri

      @http_timeout = opts[:http_timeout] || 2 * 60
    end

    def send(query)
      uri = broker_uri(query)

      headers = {}
      headers[X_REQUEST_ID] = RequestStore.store[:h4_request_id] if
        RequestStore.store[:h4_request_id]
      headers[X_ILB_REQUEST_ID] = RequestStore.store[:ilb_request_id] if
        RequestStore.store[:ilb_request_id]
      headers['Content-Type'] = 'application/json'

      req = Net::HTTP::Post.new(uri.path, headers)
      req.body = query.to_json

      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = @http_timeout
        http.request(req)
      end

      if response.code == "200"
        JSON.parse(response.body).map{ |row| ResponseRow.new(row) }
      else
        raise "Request failed: #{response.code}: #{response.body}"
      end
    end

    def query(id, &block)
      query = Query.new(id, self)
      return query unless block

      send query
    end

    def data_sources
      uri = broker_uri

      meta_path = "#{uri.path}datasources"

      req = Net::HTTP::Get.new(meta_path)

      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = @http_timeout
        http.request(req)
      end

      if response.code == "200"
        JSON.parse(response.body)
      else
        raise "Request failed: #{response.code}: #{response.body}"
      end
    end

    # H4Druid::Client#broker_uri needs query to be passed to it.
    def broker_uri(_query = nil)
      URI(@broker_url) if @broker_url
    rescue
      nil
    end

    def data_source(source)
      uri = broker_uri

      meta_path = "#{uri.path}datasources/#{source.split('/').last}"

      req = Net::HTTP::Get.new(meta_path)

      response = Net::HTTP.new(uri.host, uri.port).start do |http|
        http.read_timeout = @http_timeout
        http.request(req)
      end

      if response.code == "200"
        meta = JSON.parse(response.body)
        meta.define_singleton_method(:dimensions) { self['dimensions'] }
        meta.define_singleton_method(:metrics) { self['metrics'] }
        meta
      else
        raise "Request failed: #{response.code}: #{response.body}"
      end
    end
  end
end
