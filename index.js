/**
 * Created by woohuiren on 29/7/17.
 */
const Twit = require('twit'),
    sqlite3 = require('sqlite3').verbose(),
    db = new sqlite3.Database('dm2tweet.sqlite3');

require('dotenv').config();

// Prepare SQLite Database
db.serialize(() => {
    db.run('CREATE TABLE IF NOT EXISTS tweets(id INTEGER PRIMARY KEY AUTOINCREMENT, message TINYTEXT, tweeted TINYINT(1) DEFAULT 0, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL)');
    db.run('CREATE INDEX IF NOT EXISTS tweets_tweeted_index ON tweets (tweeted)');
    db.run('CREATE INDEX IF NOT EXISTS tweets_created_at_index ON tweets (created_at)');
});

// Initialize Twitter API keys and other constants
const twitter = new Twit({
    consumer_key: process.env.CONSUMER_KEY,
    consumer_secret: process.env.CONSUMER_SECRET,
    access_token: process.env.ACCESS_TOKEN,
    access_token_secret: process.env.ACCESS_SECRET
});

const settings = {
    verbose_output:  process.env.VERBOSE_OUTPUT || false,
    max_tweet_char: process.env.MAX_TWEET_CHAR || 130,
    tweet_interval: process.env.TWEET_INTERVAL || 120,
    max_tweets_daily: process.env.MAX_TWEETS_DAILY || 1000
};

// Verify the credentials
twitter.get('/account/verify_credentials', (error, data) => {

    // if there was an error authenticating, bail
    if (error) {
        console.error(JSON.stringify(error));
        throw error;
    }

    if (settings.verbose_output) {
        console.log('credential verification: ' + JSON.stringify(data));
    }

});

// Twitter DM streaming
const stream = twitter.stream('user');

stream.on('direct_message', (directMessage) => {
    const current_message = directMessage.direct_message;

    if (settings.verbose_output) {
        console.log(JSON.stringify(current_message));
    }

    // Make sure a message exists before continuing
    if (current_message) {
        // Check message length
        if (current_message.text.length <= settings.max_tweet_char) {
            // Insert into tweet queue
            const stmt = db.prepare('INSERT INTO tweets (message) VALUES (?)');
            stmt.run(current_message.text);
            stmt.finalize();
        }
    } else {
        console.warn('No message to read');
    }
});

// Queue processing for tweets
const tweetDm = () => {
    // Check if it has exceed max tweets per day yet
    db.get('SELECT COUNT(*) AS count FROM tweets WHERE tweeted = 1 AND DATE(created_at) < DATE(\'now\', \'-1 day\')', (countErr, countRow) => {
        if (!countErr) {
            if (countRow) {
                if (countRow.count <= settings.max_tweets_daily) {
                    // Retrieve the first message from SQLite
                    db.get('SELECT * FROM tweets WHERE tweeted = 0', (err, row) => {
                        if (!err) {
                            if (row) {
                                const tweet = row.id + '. ' + row.message;

                                twitter.post('statuses/update', {
                                    status: tweet,
                                    possibly_sensitive: true
                                }, (tweetError, tweet) => {

                                    if (tweetError) {
                                        console.error('Tweet error: ' + JSON.stringify(tweetError));
                                    } else {
                                        // Mark tweet as tweeted
                                        const stmt = db.prepare('UPDATE tweets SET tweeted = 1 WHERE id = ?');
                                        stmt.run(row.id);
                                        stmt.finalize();
                                    }

                                    if (settings.verbose_output) {
                                        console.log(JSON.stringify(tweet));
                                    }

                                });
                            }
                        } else {
                            console.error('DB tweet retrieval error: ' + JSON.stringify(err));
                        }
                    });
                } else {
                    console.error('Tweet limit exceeded!');
                }
            } else {
                console.error('DB tweets count has no row!');
            }
        } else {
            console.error('DB tweets count retrieval error: ' + JSON.stringify(countErr));
        }
    });
};
// Create the queue
const tweetQueue = setInterval(tweetDm, settings.tweet_interval * 1000);

// Handle exit signals
process.on('SIGINT', () => {
    db.close();
    process.exit(1);
});

process.on('exit', () => {
    console.log('Exiting...');
    db.close();
});