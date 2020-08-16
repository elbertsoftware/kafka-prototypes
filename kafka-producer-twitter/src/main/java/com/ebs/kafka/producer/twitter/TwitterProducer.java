package com.ebs.kafka.producer.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private static final Logger logger = LoggerFactory.getLogger(TwitterProducer.class);

    private static final String bootstrapServer = "localhost:9092";
    private static final String topicName = "twitter_tweets";  // use KafkaTool or CLI to create the new topic before running the program

    private Client createTwitterClient(BlockingQueue<String> msgQueue) {
        List<String> terms = Lists.newArrayList("kafka", "elasticsearch");

        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(terms);

        // move these to properties file later
        String consumerKey = "lWKlpQOf36nrauOiIpzsl2Wf8";
        String consumerSecret = "of0Yj4tlcl5eYjBnp87YpESSts8RjWJfLx5ZnMINamEXHTT9py";
        String token = "1294757003156320257-bes3QLKciD8WGJzXH5P7nLhsd76ecY";
        String secret = "Ah1ydFr2Fnl251cWKA232veYnCANVJd4SvsehAMj7yfUk";
        Authentication auth = new OAuth1(consumerKey, consumerSecret, token, secret);

        Hosts hosts = new HttpHosts(Constants.STREAM_HOST);
        ClientBuilder builder = new ClientBuilder()
                .name("Twitter-Producer-01")  // optional: mainly for the logs
                .hosts(hosts)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(msgQueue));

        return builder.build();  // return the client object
    }

    private KafkaProducer<String, String> createKafkaProducer(String bootstrapServer) {
        Properties configs = new Properties();

        // basic settings
        configs.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        configs.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configs.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe producer (idempotent)
        configs.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configs.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        configs.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        configs.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");  // Since Kafka version is >= 1.1

        // enable high throughput producer at the expense of a bit of latency and CPU usage
        // no need to modify consumers since they know how to decompress
        configs.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");  // great for text/json based messages
        configs.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");  // 20 milliseconds
        configs.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));  // 32KB

        return new KafkaProducer<>(configs);  // return Producer object
    }

    private void run() {
        logger.info("TwitterProducer started");

        // create twitter client
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(100);
        Client twitter = createTwitterClient(msgQueue);
        twitter.connect();

        // create kafka producer
        KafkaProducer producer = createKafkaProducer(bootstrapServer);

        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            close(twitter, producer);
        }));

        // loop to send tweets to kafka
        try {
            while (!twitter.isDone()) {
                String msg = msgQueue.poll(5L, TimeUnit.SECONDS);
                if (msg != null) {
                    logger.info(msg);

                    producer.send(new ProducerRecord<>(topicName, msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if (e != null) {
                                logger.error("Failed to produce message to Kafka", e);
                            }
                        }
                    });
                }
            }
        } catch (InterruptedException e) {
            logger.error("Failed to poll messages from Twitter", e);
        } finally {
            close(twitter, producer);
        }

        // stop twitter client
        logger.info("TwitterProducer exited");
    }

    private void close(Client twitter, KafkaProducer<String, String> producer) {
        logger.info("Closing Twitter client...");
        twitter.stop();

        logger.info("Flushing Twitter producer...");
        producer.close();
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }
}
/*
Sample tweets in JSON format:
{
   "created_at":"Sun Aug 16 03:48:10 +0000 2020",
   "id":1294843366992048129,
   "id_str":"1294843366992048129",
   "text":"@elbertsoftware Test tweet with word Kafka and ElasticSearch",
   "source":"\u003ca href=\"https:\/\/mobile.twitter.com\" rel=\"nofollow\"\u003eTwitter Web App\u003c\/a\u003e",
   "truncated":false,
   "in_reply_to_status_id":null,
   "in_reply_to_status_id_str":null,
   "in_reply_to_user_id":1294757003156320257,
   "in_reply_to_user_id_str":"1294757003156320257",
   "in_reply_to_screen_name":"elbertsoftware",
   "user":{
      "id":1294757003156320257,
      "id_str":"1294757003156320257",
      "name":"Kenneth Pham",
      "screen_name":"elbertsoftware",
      "location":"Riverside, CA",
      "url":null,
      "description":"Professional software solution provider",
      "translator_type":"none",
      "protected":false,
      "verified":false,
      "followers_count":0,
      "friends_count":0,
      "listed_count":0,
      "favourites_count":0,
      "statuses_count":1,
      "created_at":"Sat Aug 15 22:05:19 +0000 2020",
      "utc_offset":null,
      "time_zone":null,
      "geo_enabled":false,
      "lang":null,
      "contributors_enabled":false,
      "is_translator":false,
      "profile_background_color":"F5F8FA",
      "profile_background_image_url":"",
      "profile_background_image_url_https":"",
      "profile_background_tile":false,
      "profile_link_color":"1DA1F2",
      "profile_sidebar_border_color":"C0DEED",
      "profile_sidebar_fill_color":"DDEEF6",
      "profile_text_color":"333333",
      "profile_use_background_image":true,
      "profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/1294759216247263232\/nY5kXM_H_normal.jpg",
      "profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/1294759216247263232\/nY5kXM_H_normal.jpg",
      "profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/1294757003156320257\/1597529723",
      "default_profile":true,
      "default_profile_image":false,
      "following":null,
      "follow_request_sent":null,
      "notifications":null
   },
   "geo":null,
   "coordinates":null,
   "place":null,
   "contributors":null,
   "is_quote_status":false,
   "quote_count":0,
   "reply_count":0,
   "retweet_count":0,
   "favorite_count":0,
   "entities":{
      "hashtags":[

      ],
      "urls":[

      ],
      "user_mentions":[
         {
            "screen_name":"elbertsoftware",
            "name":"Kenneth Pham",
            "id":1294757003156320257,
            "id_str":"1294757003156320257",
            "indices":[
               0,
               15
            ]
         }
      ],
      "symbols":[

      ]
   },
   "favorited":false,
   "retweeted":false,
   "filter_level":"low",
   "lang":"en",
   "timestamp_ms":"1597549690374"
}
 */