describe Druid::Client do

  it 'validates URI on intialize' do
    expect { Druid::Client.new('test_uri\abc') }.to raise_error(StandardError, /Invalid broker url/)
  end

  it 'broker_uri returns broker URI object' do
    client = Druid::Client.new('test_uri')
    expect(client.broker_uri).to be_a URI
    expect(client.broker_uri.to_s).to eq 'test_uri'
  end

  it 'creates a query' do
    Druid::Client.new('test_uri').query('test/test').should be_a Druid::Query
  end

  it 'sends query if block is given' do
    client = Druid::Client.new('test_uri')
    client.should_receive(:send)
    client.query('test/test') do
      group(:group1)
    end
  end

  it 'parses response on 200' do
    stub_request(:post, "http://www.example.com/druid/v2").
      with(:body => "{\"dataSource\":\"test\",\"granularity\":\"all\",\"intervals\":[\"2013-04-04T00:00:00+00:00/2013-04-04T00:00:00+00:00\"]}",
      :headers => {'Accept'=>'*/*', 'Content-Type'=>'application/json', 'User-Agent'=>'Ruby'}).
      to_return(:status => 200, :body => "[]", :headers => {})

    client = Druid::Client.new('http://www.example.com/druid/v2')
    JSON.should_receive(:parse).and_return([])
    client.send(client.query('test/test').interval("2013-04-04", "2013-04-04"))
  end

  it 'raises on request failure' do
    stub_request(:post, "http://www.example.com/druid/v2").
      with(:body => "{\"dataSource\":\"test\",\"granularity\":\"all\",\"intervals\":[\"2013-04-04T00:00:00+00:00/2013-04-04T00:00:00+00:00\"]}",
      :headers => {'Accept'=>'*/*', 'Content-Type'=>'application/json', 'User-Agent'=>'Ruby'}).
      to_return(:status => 666, :body => "Strange server error", :headers => {})

    client = Druid::Client.new('http://www.example.com/druid/v2')
    expect { client.send(client.query('test/test').interval("2013-04-04", "2013-04-04")) }.to raise_error(RuntimeError, /Request failed: 666: Strange server error/)
  end


  it 'should report list of all data sources correctly' do
    stub_request(:get, "http://www.example.com/druid/v2/datasources").
      with(:headers =>{'Accept'=>'*/*', 'User-Agent'=>'Ruby'}).
      to_return(:status => 200, :body => '["ds_1","ds_2","ds_3"]')

    client = Druid::Client.new('http://www.example.com/druid/v2/')
    expect(client.data_sources).to eq ["ds_1","ds_2","ds_3"]
  end

  it 'should report dimensions of a data source correctly' do
    stub_request(:get, "http://www.example.com/druid/v2/datasources/mock").
      with(:headers =>{'Accept'=>'*/*', 'User-Agent'=>'Ruby'}).
      to_return(:status => 200, :body => '{"dimensions":["d1","d2","d3"],"metrics":["m1", "m2"]}')

    client = Druid::Client.new('http://www.example.com/druid/v2/')
    client.data_source('madvertise/mock').dimensions.should == ["d1","d2","d3"]
  end

  it 'should report metrics of a data source correctly' do
    stub_request(:get, "http://www.example.com/druid/v2/datasources/mock").
      with(:headers =>{'Accept'=>'*/*', 'User-Agent'=>'Ruby'}).
      to_return(:status => 200, :body => '{"dimensions":["d1","d2","d3"],"metrics":["m1", "m2"]}')

    client = Druid::Client.new('http://www.example.com/druid/v2/')
    client.data_source('madvertise/mock').metrics.should == ["m1","m2"]
  end

end
