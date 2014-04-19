require 'cql'
require 'msgpack'
require 'json'

module Fluent
  class CqlOutput < BufferedOutput
    Fluent::Plugin.register_output('cql', self)

    config_param :host,          :string
    config_param :port,          :integer
    config_param :keyspace,      :string
    config_param :columnfamily,  :string
    config_param :ttl,           :integer, default: 0
    config_param :schema,        :string
    config_param :data_keys,     :string

    # remove keys from the fluentd json event as they're processed
    # for individual columns?
    config_param :pop_data_keys, :bool, default: true

    def connection
      @connection ||= get_connection(self.host, self.port, self.keyspace)
    end

    def configure(conf)
      super

      # perform validations
      raise ConfigError, "'Host' is required by Cassandra output (ex: localhost, 127.0.0.1, ec2-54-242-141-252.compute-1.amazonaws.com" if self.host.nil?
      raise ConfigError, "'Port' is required by Cassandra output (ex: 9042)" if self.port.nil?
      raise ConfigError, "'Keyspace' is required by Cassandra output (ex: FluentdLoggers)" if self.keyspace.nil?
      raise ConfigError, "'ColumnFamily' is required by Cassandra output (ex: events)" if self.columnfamily.nil?
      raise ConfigError, "'Schema' is required by Cassandra output (ex: id,ts,payload)" if self.schema.nil?
      raise ConfigError, "'Schema' must contain at least two column names (ex: id,ts,payload)" if self.schema.split(',').count < 2
      raise ConfigError, "'DataKeys' is required by Cassandra output (ex: tag,created_at,data)" if self.data_keys.nil?

      # convert schema from string to hash
      # NOTE: ok to use eval b/c this isn't this isn't a user
      #       supplied string
      self.schema = eval(self.schema)

      # convert data keys from string to array
      self.data_keys = self.data_keys.split(',')
    end

    def start
      super
      connection
    end

    def shutdown
      super
    end

    def format(tag, time, record)
      record.to_msgpack
    end

    def write(chunk)
      chunk.msgpack_each  do |record|
        values = build_insert_values_string(self.schema.keys, self.data_keys, record, self.pop_data_keys)
        if values.class == Array
          values.each do |val|
            insert_to_db(val, @connection)
          end
        else
          insert_to_db(values, @connection)
        end
      end
    end

    private

    def insert_to_db(val, con)
      cql = "INSERT INTO #{self.columnfamily} (#{self.schema.keys.join(',')}) " +
                          "VALUES (#{val}) " +
                          "USING TTL #{self.ttl}"
      con.execute(cql)
    end

    def get_connection(host, port, keyspace)
      client = ::Cql::Client.connect(hosts: [host], port: port.to_i, default_consistency: :one)
      client.use("\"#{keyspace}\"")
      client
    end

    def process_value(index, record, key, schema_keys)
      return 'now()' if key == 'id' # id should be timeuuid
      if pop_data_keys
        case schema[schema_keys[index]]
        when :string, :text
          "'#{record.delete(key)}'"
        when :map
          record.delete(key).inject({}) do |res,kv|
            res.merge!({ kv[0].to_s => kv[1].to_s })
          end.to_s.gsub('"',"'").gsub('=>',':')
        else
          record.delete(key)
        end
      else
        case schema[schema_keys[index]]
        when :string, :text
          "'#{record[key]}'"
        when :map
          record[key].inject({}) do |res,kv|
            res.merge!({ kv[0].to_s => kv[1].to_s })
          end.to_s.gsub('"',"'").gsub('=>',':')
        else
          record[key]
        end
      end
    end

    def gen_row_data(record, data_keys, schema_keys)
      values = data_keys.map.with_index do |key, index|
        process_value(index, record, key, schema_keys)
      end
      # if we have one more schema key than data keys,
      # we can then infer that we should store the event
      # as a string representation of the corresponding
      # json object in the last schema column
      if schema_keys.count == data_keys.count + 1
        values << if record.count > 0
                    "'#{record.to_json}'"
                  else
                    # by this point, the extra schema column has been
                    # added to insert cql statement, so we must put
                    # something in it
                    # TODO: detect this scenario earlier and don't
                    #       specify the column name/value at all
                    #       when constructing the cql stmt
                    "''"
                  end
      end

      values.join(',')
    end

    def build_insert_values_string(schema_keys, data_keys, record, pop_data_keys)
      if record.class == Array
        record.map do |r|
          gen_row_data(r, data_keys, schema_keys)
        end
      else
        gen_row_data(record, data_keys, schema_keys)
      end
    end
  end
end
