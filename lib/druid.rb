require 'time'
require 'json'

module Druid
  class Query

    def initialize(source)
      @properties = {}
      data_source(source)
    end

    def query_type(type)
      @properties[:queryType] = type
      self
    end

    def data_source(source)
      @properties[:dataSource] = source
      self
    end

    def group(*dimensions)
      query_type(:groupBy)
      @properties[:dimensions] = dimensions.flatten
      self
    end

    [:long_sum, :double_sum].each do |method_name|
      agg_type = method_name.to_s.split('_')
      agg_type[1].capitalize!
      agg_type = agg_type.join

      define_method method_name do |*metrics|
        aggregations = (@properties[:aggregations] || []).select{|agg| agg[:type] != agg_type }
        aggregations.concat(metrics.flatten.map{ |metric|
          {
            :type => agg_type,
            :name => metric,
            :fieldName => metric
          }
        })
        @properties[:aggregations] = aggregations
        self
      end
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

    def interval(from, to)
      from = DateTime.parse(from.to_s) unless from.respond_to? :iso8601
      to = DateTime.parse(to.to_s) unless to.respond_to? :iso8601
      @properties[:intervals] = ["#{from.iso8601}/#{to.iso8601}"]
      self
    end

    def granularity(gran)
      @properties[:granularity] = gran.to_s
      self
    end

    def to_json
      @properties.to_json
    end

  end

  class FilterParameter
    (instance_methods + private_instance_methods).each do |method|
      unless method.to_s =~ /^(__|instance_eval|instance_exec|initialize|object_id|raise|puts|inspect)/ || method.to_s =~ /\?/
        undef_method method
      end
    end
  end

  class Filter
    (instance_methods + private_instance_methods).each do |method|
      unless method.to_s =~ /^(__|instance_eval|instance_exec|initialize|object_id|raise|puts|inspect)/ || method.to_s =~ /\?/
        undef_method method
      end
    end

    def method_missing(method_id, *args)
      FilterDimension.new(method_id)
    end
  end

  class FilterDimension < FilterParameter
    def initialize(name)
      @name = name
      @value = nil
    end

    def eq(value)
      return self.in(value) if value.is_a? Array
      @value = value
      self
    end

    alias :'==' :eq


    def neq(value)
      return !self.in(value)
    end

    alias :'!=' :neq

    def in(*args)
      values = args.flatten
      raise "Must provide non-empty array in in()" if values.empty?

      if (values.length == 1)
        @value = values[0]
        return self
      end

      filter_or = FilterOperator.new('or', true)
      values.each do |value|
        raise "query is too complex" if value.is_a? FilterParameter
        param = FilterDimension.new(@name)
        param.eq value
        filter_or.add param
      end
      filter_or
    end

    def &(other)
      filter_and = FilterOperator.new('and', true)
      filter_and.add(self)
      filter_and.add(other)
      filter_and
    end

    def |(other)
      filter_or = FilterOperator.new('or', true)
      filter_or.add(self)
      filter_or.add(other)
      filter_or
    end

    def !()
      filter_not = FilterOperator.new('not', false)
      filter_not.add(self)
      filter_not
    end

    def to_hash
      raise 'no value assigned' if @value.nil?
      {
        :type => 'selector',
        :dimension => @name,
        :value => @value
      }
    end

    def to_s
      to_hash.to_s
    end

    def to_json(*a)
      to_hash.to_json(*a)
    end
  end

  class FilterOperator < FilterParameter
    def initialize(name, takes_many)
      @name = name
      @takes_many = takes_many
      @elements = []
    end

    def add(element)
      @elements.push element
    end

    def &(other)
      if @name == 'and'
        filter_and = self
      else
        filter_and = FilterOperator.new('and', true)
        filter_and.add(self)
      end
      filter_and.add(other)
      filter_and
    end

    def |(other)
      if @name == 'or'
        filter_or = self
      else
        filter_or = FilterOperator.new('or', true)
        filter_or.add(self)
      end
      filter_or.add(other)
      filter_or
    end

    def !()
      if @name == 'not'
        @elements[0]
      else
        filter_not = FilterOperator.new('not', false)
        filter_not.add(self)
        filter_not
      end
    end

    def to_json(*a)
      to_hash.to_json(*a)
    end

    def to_s
      to_hash.to_s
    end

    def as_json(options)
      to_hash
    end


    def to_hash
      result = {
        :type => @name
      }
      if @takes_many
        result[:fields] = @elements.map(&:to_hash)
      else
        result[:field] = @elements[0].to_hash
      end
      result
    end
  end
end