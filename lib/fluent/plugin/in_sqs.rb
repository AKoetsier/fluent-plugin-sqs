module Fluent

  require 'aws-sdk'

  class SQSInput < Input
    Plugin.register_input('sqs', self)

    unless method_defined?(:router)
      define_method("router") { Fluent::Engine }
    end

    def initialize
      super
    end

    config_param :aws_key_id, :string, :default => nil, :secret => true
    config_param :aws_sec_key, :string, :default => nil, :secret => true
    config_param :tag, :string
    config_param :sqs_endpoint, :string, :default => 'sqs.ap-northeast-1.amazonaws.com'
    config_param :sqs_url, :string
    config_param :receive_interval, :time, :default => 0.1
    config_param :max_number_of_messages, :integer, :default => 10
    config_param :wait_time_seconds, :integer, :default => 10

    def configure(conf)
      super

    end

    def start
      super

      @client = Aws::SQS::Client.new(
        :access_key_id => @aws_key_id,
        :secret_access_key => @aws_sec_key,
        :endpoint => @sqs_endpoint
        )

      @finished = false
      @thread = Thread.new(&method(:run_periodic))
    end

    def shutdown
      super

      @finished = true
      @thread.join
    end

    def run_periodic
      until @finished
        begin
          sleep @receive_interval
          resp = @client.receive_message(
            :queue_url => @sqs_url,
            :max_number_of_messages => @max_number_of_messages,
            :wait_time_seconds => @wait_time_seconds
          )
          resp.messages.each do |message|
            record = {}
            record['body'] = message.body
            record['handle'] = message.receipt_handle
            record['id'] = message.message_id
            record['md5'] = message.md5_of_body
            record['url'] = @sqs_url
            record['sender_id'] = message.attributes['SenderId']

            router.emit(@tag, Time.now.to_i, record)

            @client.delete_message({
              queue_url: @sqs_url,
              receipt_handle: message.receipt_handle
            })
          end
        rescue
          $log.error "failed to emit or receive", :error => $!.to_s, :error_class => $!.class.to_s
          $log.warn_backtrace $!.backtrace
        end
      end
    end
  end
end
