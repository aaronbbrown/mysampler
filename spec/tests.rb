require 'timecop'
require 'mysqlsampler'


describe "database connectivity" do
  before(:all) do
    @ms = MySQLSampler.new
    @ms.user     = "aaron"
    @ms.pass     = "n0m0r3181"
    @ms.host     = "locutus.borg.lan"
    @ms.port     = 3306
    @ms.db_connect
  end

  it "should return 'locutus.borg.lan'" do
    @ms.get_mysql_hostname.should == @ms.host 
  end

  it "should return an array that includes 'Uptime'" do
    @ms.get_header_rows.should include('Uptime')
  end

end

describe "Row operations" do
  before(:all) do
    Timecop.freeze(Time.local(2011,10,17,0,0,0))
    @ms = MySQLSampler.new
  end

  after(:all) do
    Timecop.return
  end

  # test relative operation
  it "should return a key with 2 as the value" do
    @ms.calc_relative({"Foo" => 4})
    h = @ms.calc_relative({"Foo" => 6})
    h["Foo"].should == 2
  end

  it "should prefix key with 'foo.'" do
    h = @ms.prefix_keys({"bar" => 1}, "foo.")
    h.keys.first.should == "foo.bar"
  end

  it "should convert a string to a numeric if it is a number" do
    @ms.to_numeric("5").should == 5
  end

  it "should not convert a string to a numeric if it is a string" do
    @ms.to_numeric("a").should == "a"
  end

  it "should convert a hash of string numbers to numbers" do
    h = @ms.values_to_numeric({"a" => "1"})
    h["a"].should == 1
  end

  it "should convert a hash into a comma separated csv prefixed by time" do
    h = { "a" => 1, "b" => 2, "c" => "foo" }
    @ms.hash_to_csv(h).should == "2011-10-17 00:00:00,1,2,foo"
  end

end
