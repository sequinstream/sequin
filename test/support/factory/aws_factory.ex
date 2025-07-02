defmodule Sequin.Factory.AwsFactory do
  @moduledoc false

  def sns_publish_batch_response_success do
    AWS.XML.encode_to_iodata!(%{
      "PublishBatchResponse" => %{
        "PublishBatchResult" => %{
          "Failed" => "",
          "Successful" => %{
            "member" => [
              %{
                "Id" => "23df4256-b838-4538-ad38-b4231aa2b71c",
                "MessageId" => "73260025-473a-5494-b7d2-6055e8dd4bfd",
                "SequenceNumber" => "10000000000000003000"
              }
            ]
          }
        },
        "ResponseMetadata" => %{
          "RequestId" => "abbf754a-da6a-557c-865b-153a6b69bc16"
        }
      }
    })
  end

  def sns_publish_batch_response_failure do
    AWS.XML.encode_to_iodata!(%{
      "PublishBatchResponse" => %{
        "PublishBatchResult" => %{
          "Failed" => %{
            "member" => [
              %{
                "Id" => "23df4256-b838-4538-ad38-b4231aa2b71c",
                "Code" => "InternalError",
                "Message" => "Internal Error occurred",
                "SenderFault" => true
              }
            ]
          },
          "Successful" => %{
            "member" => [
              %{
                "Id" => "23df4256-b838-4538-ad38-b4231aa2b71c",
                "MessageId" => "73260025-473a-5494-b7d2-6055e8dd4bfd",
                "SequenceNumber" => "10000000000000003000"
              }
            ]
          }
        },
        "ResponseMetadata" => %{
          "RequestId" => "abbf754a-da6a-557c-865b-153a6b69bc16"
        }
      }
    })
  end

  def sns_get_topic_attributes_response_success do
    AWS.XML.encode_to_iodata!(%{
      "Attributes" => %{
        "entry" => [
          %{
            "key" => "TopicArn",
            "value" => "arn:aws:sns:us-east-2:689238261712:testing.fifo"
          },
          %{"key" => "FifoTopic", "value" => "true"},
          %{"key" => "DisplayName", "value" => ""},
          %{"key" => "ContentBasedDeduplication", "value" => "false"},
          %{"key" => "FifoThroughputScope", "value" => "MessageGroup"},
          %{"key" => "SubscriptionsConfirmed", "value" => "0"}
        ]
      }
    })
  end

  def sns_list_topics_response_success do
    AWS.XML.encode_to_iodata!(%{
      "ListTopicsResponse" => %{
        "Topics" => %{
          "member" => [
            %{
              "TopicArn" => "arn:aws:sns:us-east-1:123456789012:test-topic-1"
            },
            %{
              "TopicArn" => "arn:aws:sns:us-east-1:123456789012:test-topic-2"
            }
          ]
        },
        "ResponseMetadata" => %{
          "RequestId" => "e9b5b9e5-5b5b-5b5b-5b5b-5b5b5b5b5b5b"
        }
      }
    })
  end
end
