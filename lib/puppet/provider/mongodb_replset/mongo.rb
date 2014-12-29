#
# Author: François Charlier <francois.charlier@enovance.com>
#

Puppet::Type.type(:mongodb_replset).provide(:mongo) do

  desc "Manage hosts members for a replicaset."

  confine :true =>
    begin
      require 'json'
      true
    rescue LoadError
      false
    end

  commands :mongo => 'mongo'

  mk_resource_methods

  def initialize(resource={})
    super(resource)
    @property_flush = {}
  end

  def self.members_cleanup(members)
    defaults = {:arbiterOnly => false, :hidden => false, :priority => 1, :slaveDelay => 0}
    if members == nil
      members = {}
    end
    if members.kind_of?(Array)
      members = Hash[members.map {|v| [v,defaults]}]
    end

    members.each do |member, parameters|
      if ! parameters.kind_of?(Hash)
        members[member] = defaults
      else
        parameters.merge!(defaults) { |key, v1 ,v2| v1 }
      end
    end
    members
  end

  def members=(hosts)
    hosts = self.class.members_cleanup(hosts)
    @property_flush[:members] = hosts
  end

  def self.instances
    instance = get_replset_properties
    if instance
      # There can only be one replset per node
      [new(instance)]
    else
      []
    end
  end

  def self.prefetch(resources)
    instances.each do |prov|
      if resource = resources[prov.name]
        resource.provider = prov
      end
    end
  end

  def exists?
    @property_hash[:ensure] == :present
  end

  def create
    @property_flush[:ensure] = :present
    @property_flush[:members] = self.class.members_cleanup(resource[:members])
  end

  def destroy
    @property_flush[:ensure] = :absent
  end

  def flush
    set_members
    @property_hash = self.class.get_replset_properties
  end

  private

  def db_ismaster(host)
    mongo_command("db.isMaster()", host)
  end

  def rs_initiate(conf, master)
    return mongo_command("rs.initiate(#{conf})", master)
  end

  def rs_status(host)
    mongo_command("rs.status()", host)
  end

  def rs_add(host, master)
    mongo_command("rs.add(\"#{host}\")", master)
  end

  def rs_remove(host, master)
    mongo_command("rs.remove(\"#{host}\")", master)
  end

  def master_host(hosts)
    hosts.each do |host|
      status = db_ismaster(host)
      if status.has_key?('primary')
        return status['primary']
      end
    end
    false
  end

  def self.get_replset_properties
    output = mongo_command('rs.conf()')
    if output['members']
      members = output['members'].collect do |val|
        val['host']
      end
      props = {
        :name     => output['_id'],
        :ensure   => :present,
        :members  => members,
        :provider => :mongo,
      }
    else
      props = nil
    end
    Puppet.debug("MongoDB replset properties: #{props.inspect}")
    props
  end

  def alive_members(hosts)
    hosts.select do |host, params|
      begin
        Puppet.debug "Checking replicaset member #{host} ..."
        status = rs_status(host)
        if status.has_key?('errmsg') and status['errmsg'] == 'not running with --replSet'
          raise Puppet::Error, "Can't configure replicaset #{self.name}, host #{host} is not supposed to be part of a replicaset."
        end
        if status.has_key?('set')
          if status['set'] != self.name
            raise Puppet::Error, "Can't configure replicaset #{self.name}, host #{host} is already part of another replicaset."
          end

          # This node is alive and supposed to be a member of our set
          Puppet.debug "Host #{self.name} is available for replset #{status['set']}"

          true
        elsif status.has_key?('info')
          Puppet.debug "Host #{self.name} is alive but unconfigured: #{status['info']}"

          true
        end
      rescue Puppet::ExecutionFailure
        Puppet.warning "Can't connect to replicaset member #{host}."

        false
      end
    end
  end

  def set_members
    if @property_flush[:ensure] == :absent
      # TODO: I don't know how to remove a node from a replset; unimplemented
      #Puppet.debug "Removing all members from replset #{self.name}"
      #@property_hash[:members].collect do |member|
      #  rs_remove(member, master_host(@property_hash[:members]))
      #end
      return
    end

    if ! @property_flush[:members].empty?
      # Find the alive members so we don't try to add dead members to the replset
      alive_hosts = alive_members(@property_flush[:members])
      dead_hosts  = Hash[@property_flush[:members].to_a - alive_hosts.to_a]
      raise Puppet::Error, "Can't connect to any member of replicaset #{self.name}." if alive_hosts.empty?
      Puppet.debug "Alive members: #{alive_hosts.inspect}"
      Puppet.debug "Dead members: #{dead_hosts.inspect}" unless dead_hosts.empty?
    else
      alive_hosts = {}
    end

    if @property_flush[:ensure] == :present and @property_hash[:ensure] != :present
      Puppet.debug "Initializing the replset #{self.name}"

      # Create a replset configuration
      id = -1
      hostconf = alive_hosts.each.map do |host,values|
        id = id + 1
        "{ _id: #{id}, host: \"#{host}\", arbiterOnly: #{values[:arbiterOnly]}, hidden: #{values[:hidden]}, priority: #{values[:priority]}, slaveDelay: #{values[:slaveDelay]}}"
      end.join(',')
      conf = "{ _id: \"#{self.name}\", members: [ #{hostconf} ] }"

      # Set replset members with the first host as the master
      output = rs_initiate(conf, alive_hosts.to_a[0][0])
      if output['ok'] == 0
        raise Puppet::Error, "rs.initiate() failed for replicaset #{self.name}: #{output['errmsg']}"
      end
    else
      # Add members to an existing replset
      if master = master_host(alive_hosts)
        current_hosts_name = db_ismaster(master)['hosts'][0]
        current_hosts = {current_hosts_name => alive_hosts[current_hosts_name]}
        newhosts = Hash[alive_hosts.to_a - current_hosts.to_a]
        newhosts.each do |host|
          output = rs_add(host, master)
          if output['ok'] == 0
            raise Puppet::Error, "rs.add() failed to add host to replicaset #{self.name}: #{output['errmsg']}"
          end
        end
      else
        raise Puppet::Error, "Can't find master host for replicaset #{self.name}."
      end
    end
  end

  def mongo_command(command, host, retries=4)
    self.class.mongo_command(command,host,retries)
  end

  def self.mongo_command(command, host=nil, retries=4)
    # Allow waiting for mongod to become ready
    # Wait for 2 seconds initially and double the delay at each retry
    wait = 2
    begin
      args = Array.new
      args << '--quiet'
      args << ['--host',host] if host
      args << ['--eval',"printjson(#{command})"]
      output = mongo(args.flatten)
    rescue Puppet::ExecutionFailure => e
      if e =~ /Error: couldn't connect to server/ and wait <= 2**max_wait
        info("Waiting #{wait} seconds for mongod to become available")
        sleep wait
        wait *= 2
        retry
      else
        raise
      end
    end

    # Dirty hack to remove JavaScript objects
    output.gsub!(/ISODate\((.+?)\)/, '\1 ')
    output.gsub!(/Timestamp\((.+?)\)/, '[\1]')

    #Hack to avoid non-json empty sets
    output = "{}" if output == "null\n"

    JSON.parse(output)
  end

end
