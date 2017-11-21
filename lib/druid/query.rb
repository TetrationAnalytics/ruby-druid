require 'druid/serializable'
require 'druid/filter'
require 'druid/having'
require 'druid/post_aggregation'

require 'time'
require 'json'

module Druid
  class Query

    attr_reader :properties

    def initialize(source, client = nil)
      @properties = {}
      @client = client

      # set some defaults
      data_source(source)
      granularity(:all)

      interval(today)
    end

    def today
      Time.now.to_date.to_time
    end

    def send
      @client.send(self)
    end

    def query_type(type)
      @properties[:queryType] = type
      self
    end

    def get_query_type()
      @properties[:queryType] || :groupBy
    end

    def data_source(source)
      source = source.split('/')
      @properties[:dataSource] = source.last
      @service = source.first
      self
    end

    def source
      "#{@service}/#{@properties[:dataSource]}"
    end

    def group_by(*dimensions)
      query_type(:groupBy)
      @properties[:dimensions] = dimensions.flatten
      self
    end

    def topn(dimension, metric, threshold)
      query_type(:topN)
      @properties[:dimension] = dimension
      @properties[:metric] = metric
      @properties[:threshold] = threshold
      self
    end

    def time_series(*aggregations)
      query_type(:timeseries)
      #@properties[:aggregations] = aggregations.flatten
      self
    end

    [:long_sum, :double_sum, :count].each do |method_name|
      agg_type = method_name.to_s.split('_')
      agg_type[1].capitalize! if agg_type.length > 1
      agg_type = agg_type.join

      define_method method_name do |*metrics|
        query_type(get_query_type())
        @properties[:aggregations] = [] if @properties[:aggregations].nil?

        metrics.flatten.each do |metric|
          @properties[:aggregations] << {
            :type => agg_type,
            :name => metric.to_s,
            :fieldName => metric.to_s
          } unless contains_aggregation?(metric)
        end

        self
      end
    end

    def min(*metrics)
      query_type(get_query_type())
      @properties[:aggregations] = [] if @properties[:aggregations].nil?
      metrics.flatten.each do |metric|
        @properties[:aggregations] << {
          :type => 'min',
          :name => metric.to_s,
          :fieldName => metric.to_s
        } unless contains_aggregation?(metric)
      end

      self
    end

    def max(*metrics)
      query_type(get_query_type())
      @properties[:aggregations] = [] if @properties[:aggregations].nil?
      metrics.flatten.each do |metric|
        @properties[:aggregations] << {
            :type => 'max',
            :name => metric.to_s,
            :fieldName => metric.to_s
        } unless contains_aggregation?(metric)
      end

      self
    end

    def select(dimensions = [], metrics = [], opts = {})
      query_type(:select)
      limit = opts[:limit] || 1000
      page_token = opts[:page_token] || {}

      @properties[:dimensions] = dimensions.flatten
      @properties[:metrics] = metrics.flatten
      @properties[:pagingSpec] = {
        :pagingIdentifiers => page_token,
        :threshold => limit
      }

      self
    end

    alias_method :sum, :long_sum

    def postagg(type=:long, &block)
      post_agg = PostAggregation.new.instance_exec(&block)
      @properties[:postAggregations] ||= []
      @properties[:postAggregations] << post_agg

      # make sure, the required fields are in the query
      field_type = (type.to_s + '_sum').to_sym
      # ugly workaround, because SOMEONE overwrote send
      sum_method = self.method(field_type)
      sum_method.call(post_agg.get_field_names)

      self
    end

    def postagg_double(&block)
      postagg(:double, &block)
    end

    def filter(hash = nil, &block)
      if hash
        last = nil
        hash.each do |k,values|
          filter = FilterDimension.new(k).in(values)
          last = last ? last.&(filter) : filter
        end
        @properties[:filter] = @properties[:filter] ? @properties[:filter].&(last) : last
      end
      if block
        filter = Filter.new.instance_exec(&block)
        raise "Not a valid filter" unless filter.is_a? FilterParameter
        @properties[:filter] = @properties[:filter] ? @properties[:filter].&(filter) : filter
      end
      self
    end

    def interval(from, to = Time.now)
      intervals([[from, to]])
    end

    def intervals(is)
      @properties[:intervals] = is.map{ |ii| mk_interval(ii[0], ii[1]) }
      self
    end

    def having(&block)
      having = Having.new.instance_exec(&block)

      if old_having = @properties[:having]
        if old_having.operator? && old_having.and?
          new_having = old_having
        else
          new_having = HavingOperator.new('and')
          new_having.add(old_having)
        end
        new_having.add(having)
      else
        new_having = having
      end

      @properties[:having] = new_having
      self
    end

    alias_method :[], :interval

    def granularity(gran, time_zone = nil)
      gran = gran.to_s
      case gran
      when 'none', 'all', 'second', 'minute', 'fifteen_minute', 'thirty_minute', 'hour'
        @properties[:granularity] = gran
        return self
      when 'day'
        gran = 'P1D'
      end

      time_zone ||= Time.now.strftime('%Z')
      # druid doesn't seem to understand 'CEST'
      # this is a work around
      time_zone = 'Europe/Berlin' if time_zone == 'CEST'

      @properties[:granularity] = {
        :type => 'period',
        :period => gran,
        :timeZone => time_zone
      }
      self
    end

    def duration_granularity(duration_msec, time_zone = nil)
      time_zone ||= Time.now.strftime('%Z')
      @properties[:granularity] = {
        :type => 'duration',
        :duration => duration_msec,
        :timeZone => time_zone
      }
      self
    end

    def distinct_count(dimensions)
      dimensions.each do |dimension|
        @properties[:aggregations] << {
          :type => "hyperUnique",
          :name => dimension.to_s,
          :fieldName => dimension
        }
      end
      self
    end

    def to_json
      @properties.to_json
    end

    def limit_spec(limit, columns)
      @properties[:limitSpec] = {
        :type => :default,
        :limit => limit,
        :columns => order_by_column_spec(columns)
      }
      self
    end

    private

    def order_by_column_spec(columns)
      columns.map do |dimension, direction|
        {
          :dimension => dimension,
          :direction => direction
        }
      end
    end

    def mk_interval(from, to)
      from = today + from if from.is_a?(Integer)
      to = today + to if to.is_a?(Integer)

      from = DateTime.parse(from.to_s) unless from.respond_to? :iso8601
      to = DateTime.parse(to.to_s) unless to.respond_to? :iso8601
      "#{from.iso8601}/#{to.iso8601}"
    end

    def contains_aggregation?(metric)
      return false if @properties[:aggregations].nil?
      @properties[:aggregations].index { |aggregation| aggregation[:fieldName] == metric.to_s }
    end
  end

end
