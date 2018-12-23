# E6893 Big Data Analytics: Trending Products Acquisition and Tuning for E-commerce

# Running twitter_stream.py

## Dependencies


1. Appropriately set env vars, either in the shell, or in a `.env` file that you can create in the `final_project` folder.  Example contents of .env file:

   ```bash
   TWITTER_ACCESS_SECRET=<use your own>
   TWITTER_ACCESS_TOKEN=<use your own>
   TWITTER_CONSUMER_KEY=<use your own>
   TWITTER_CONSUMER_SECRET=<use your own>
   PYSPARK_DRIVER_PYTHON=python3
   PYSPARK_PYTHON=python3
   TERM=xterm
   ```

1. pip

```bash
$ cd final_project
$ pip install --user pipenv
$ echo export PATH=\$PATH:$(python -m site --user-base)/bin | tee -a ~/.bashrc ~/.bash_profile && source ~/.bashrc ~/.bash_profile
$ pipenv install && pipenv run python twitter_stream.py
```

## Check # of tweets:

```bash
$ cd final_project
$ pipenv install
$ pipenv run python get_table_count.py
```

## HBase Schema

* Table Name: tweets
* Total Columns: 64
* Column Families: 'user','tweet'

### Column names and sample values
```
user:profile_sidebar_fill_color: b'DDEEF6'
user:friends_count: b'\x00\x00\x00\x00\x00\x00\x07\x9c'
user:contributors_enabled: b'\x00\x00\x00\x00\x00\x00\x00\x00'
user:id_str: b'77850834'
tweet:lang: b'en'
user:favourites_count: b'\x00\x00\x00\x00\x00\x00U\xd1'
tweet:timestamp_ms: b'1544065139644'
user:profile_image_url_https: b'https://pbs.twimg.com/profile_images/983410451366273024/SMp8pvYs_normal.jpg'
tweet:in_reply_to_status_id: b'None'
tweet:in_reply_to_user_id_str: b'None'
user:protected: b'\x00\x00\x00\x00\x00\x00\x00\x00'
tweet:in_reply_to_screen_name: b'None'
user:follow_request_sent: b'None'
tweet:in_reply_to_status_id_str: b'None'
user:description: b'American #MAGA... Love video games, and never had a thought of killing people because of it either...'
user:listed_count: b'\x00\x00\x00\x00\x00\x00\x00\x02'
user:is_translator: b'\x00\x00\x00\x00\x00\x00\x00\x00'
tweet:filter_level: b'low'
user:translator_type: b'none'
user:verified: b'\x00\x00\x00\x00\x00\x00\x00\x00'
tweet:source: b'<a href="http://twitter.com/download/android" rel="nofollow">Twitter for Android</a>'
tweet:text: b"Not sure how hard it'd be. But I'd like to see @GIANTSSoftware make a game mode like @DaggerwinM survival roleplay\xe2\x80\xa6 https://t.co/GebVawq5cE"
tweet:favorited: b'\x00\x00\x00\x00\x00\x00\x00\x00'
user:following: b'None'
user:profile_link_color: b'3B94D9'
user:id: b'\x00\x00\x00\x00\x04\xa3\xe8\xd2'
tweet:contributors: b'None'
user:followers_count: b'\x00\x00\x00\x00\x00\x00\x03H'
user:default_profile_image: b'\x00\x00\x00\x00\x00\x00\x00\x00'
tweet:favorite_count: b'\x00\x00\x00\x00\x00\x00\x00\x00'
user:profile_background_image_url: b'http://abs.twimg.com/images/themes/theme2/bg.gif'
user:profile_text_color: b'333333'
tweet:created_at: b'Thu Dec 06 02:58:59 +0000 2018'
tweet:reply_count: b'\x00\x00\x00\x00\x00\x00\x00\x00'
user:lang: b'en'
user:statuses_count: b'\x00\x00\x00\x00\x00\x00\x0e%'
user:url: b'None'
user:utc_offset: b'None'
user:screen_name: b'R1V3r5Jr91_XBL_'
tweet:retweeted: b'\x00\x00\x00\x00\x00\x00\x00\x00'
user:profile_background_color: b'4A913C'
user:time_zone: b'None'
tweet:category: b'mvg'
tweet:geo: b'None'
user:profile_background_image_url_https: b'https://abs.twimg.com/images/themes/theme2/bg.gif'
tweet:place: b'None'
tweet:truncated: b'\x00\x00\x00\x00\x00\x00\x00\x01'
tweet:extended_tweet: b'{\'entities\: {\'hashtags\: [{\'text\: \'FarmingSimulator19\',\'indices\: [184,203]}],\'symbols\: [],\'user_mentions\: [{\'name\: \'GIANTS Software\',\'screen_name\: \'GIANTSSoftware\',\'id_str\: \'460981086\',\'indices\: [47,62],\'id\: 460981086},{\'name\: \'Daggerwin\',\'screen_name\: \'DaggerwinM\',\'id_str\: \'776006472530526208\',\'indices\: [85,96],\'id\: 776006472530526208}],\'urls\: []},\'full_text\: "Not sure how hard it\'d be. But I\'d like to see @GIANTSSoftware make a game mode like @DaggerwinM survival roleplay series. That\'s just amazing. Would love to play something like that. #FarmingSimulator19",\'display_text_range\: [0,203]}'
user:created_at: b'Sun Sep 27 22:27:48 +0000 2009'
user:profile_image_url: b'http://pbs.twimg.com/profile_images/983410451366273024/SMp8pvYs_normal.jpg'
user:profile_sidebar_border_color: b'C0DEED'
user:profile_use_background_image: b'\x00\x00\x00\x00\x00\x00\x00\x01'
user:notifications: b'None'
tweet:in_reply_to_user_id: b'None'
user:location: b'Ohio,USA'
user:name: b'Rivers Jr.'
user:geo_enabled: b'\x00\x00\x00\x00\x00\x00\x00\x01'
tweet:id: b'\x0e\xdb9\xcf\xfe\xd7\x80\x01'
user:profile_background_tile: b'\x00\x00\x00\x00\x00\x00\x00\x00'
tweet:quote_count: b'\x00\x00\x00\x00\x00\x00\x00\x00'
tweet:coordinates: b'None'
tweet:is_quote_status: b'\x00\x00\x00\x00\x00\x00\x00\x00'
tweet:retweet_count: b'\x00\x00\x00\x00\x00\x00\x00\x00'
user:default_profile: b'\x00\x00\x00\x00\x00\x00\x00\x00'
```

## Relevant docs:

*  [happybase](https://happybase.readthedocs.io/en/latest/): Python HBase library

# Proposal

### Team Members (with UNI):

* Alex Dziena (ad3363)
* Jaewon Lee (jl5102)
* Johan Sulaiman (js5063)

## Motivation
In a multi-platform e-commerce business, revenue growth is highly correlated with superior selection and low prices. We aim to create an application that scrapes Twitter and other public news sources for trending product announcements, product reviews, product promotions, and product-related articles, along with their accompanying social media reactions. 

The search will be narrowed down by assigning manually selected keywords specific to select categories (for example: Handphone, Computers, Electronics, Books, Games, Movies, etc.) existing in Blibli.com, an e-commerce company. 

The raw scraped data will then be analyzed, reported and visualized. Specifically, product Category Managers will be able to use the market intelligence to add the new trending products (SKUs), select appropriate stock levels, and set the right product price. In addition, the e-commerce website could automatically process the analytics data into a personalized recommender system that will highlight upcoming product launches in the user’s recommended product ribbon (a box on the site), based on the user’s favorite categories.

## Methodology

1. Generate Keywords
   Create keyword generators based on best-seller SKUs for relevant categories whose vendors, reviewers, and influencers typically create product buzz before launch (Handphone, Computers, Electronics, Games, Movies, Books, Media, etc.). This keyword generator will be used for searches. There is also a set of static trend keywords, such as #launch, #trend, etc., to increase desired tweet specificity to the context. This static metric keywords can be auto-generated using machine learning or other methods by picking a list of positive trend tweets that can serve as training data set, and run the machine learning model to collect more accurate keywords/phrases/hashtags that highly correlate with trending product launch.
1. Filtering
   The expected inflow of tweets will then go through a filtering process, where tweets are scrubbed and selected to increase relevance. Examples of filters: 1) Minimum Influence Index of the tweet based on minimum number of followers of the tweet author, minimum retweet/likes, 2) Freshness Index, where we set the age of the tweet to be no longer than 2-3 months, and 3) Minimum and maximum concentration of keywords per tweet (i.e.. keywords / word count), to eliminate potentially irrelevant tweets (low concentration) or artificial “buzz” tweets (high concentration / keyword-stuffing).
1. Feed Searches
   For a first step the search keywords per category will be manually picked, but we could potentially train a more automated ‘category best seller search word picker’ model. The searches are taken as inputs onto the Twitter Streaming application, along with other optional parameters such as minimum retweets received. The Twitter application will then start capturing the desired trending product information. An extended frequency of historical results storage can be set, which will give a historical view of how the products perform.  Our analysis will analyze the relevant tweets for sentiment, and ascribe positive or negative sentiment to the most popular products.
1. Data-Driven Visualization
   The bag of category-specific search keywords for each categories is used to mine product releases announcements on twitter, and the result would be a report for the Category Managers and Marketing to 1) stock these products, 2) obtain exclusive deals with manufacturers, craft the right promos, distributors and retailers, 3) set the right level of competitive prices, 4) feed into a personalized recommendation engine based on users’ past histories, etc.

## Dataset, Algorithm, and Tools
### Datasets
#### Required
* Twitter Stream

#### Optional (reviews feeds to merge with Twitter Stream)
* CNet reviews
* Ars Technica reviews
* Amazon reviews (historical)

### Algorithms
#### Classification
* Logistic Regression
* Multilayer perceptron classifier (feed-forward ANN)
* Random Forest / AdaBoosted Decision Tree

#### Clustering
* K-Means / K-Medioids / Bisecting K-Means
* GMM
* LDA

#### Text / Sentiment Analysis
* TF-IDF
* Word2Vec

#### Dimensionality reduction and Feature Selection
* Chi-Squared
* PCA

#### Model Selection and Tuning
* Cross-Validation and Train-Validation Split

### Tools
#### Intake
* Twitter API
* feedparser (RSS Feed reader)

#### Presentation and Visualization
* Jupyter Notebook
* D3 and Tableau

#### Compute and Storage
* Google Cloud Storage
* Google BigQuery
* Google Compute Engine
* Apache Spark

## Model Building Process
### Data Collection & Cleansing
* Obtain data from Twitter Streams – text data for tweets and other metrics that can show
* Process text data, address null data, and refine input variables
* Label data, including sentiment classification, for supervised learning

### Feature Selection / Engineering
* Select/add new features that have better predictive power on the outcome.  We will use chi-squared testing to test for statistically significant features and PCA to reduce dimensionality.

### Model Selection
* Test multiple models to maximize accuracy and AUROC.  This will be optimized using parameter and train/validation data split grid searches. 

### Model Validation
* Validate models to verify consistent performance

## Potential Applications / Future Work
* Build promo recommender and sales forecaster based on the list of recommended products.
* Integrate with E-commerce websites to automatically choose and add recommended products
* Use other social media and data sources to expand scope
* Check price points at other retailers to provide a pricing recommendation based on trend strength, analysis and competing market prices.

## Expected Contributions and Timeline
| Milestone | Date | Owner |
|---------- |----- |----- |
| Finalize the list of categories and associated keywords | 11/12 | Johan |
| Setup and test the extraction and staging of Twitter Streaming pipeline | 11/12 | Alex |
| Setup Filters logic in the pipeline | 11/15 | Alex |
| Prepare and train machine learning models | 11/12 | Jae |
| Capture and evaluate results of minimum 3 iterations | 11/19 | Jae |
| Create Report and Visualization for Category Managers | 11/24 | Johan |
| Finalize deliverables | 12/01 | Johan |
| Create Final Presentation | 12/05 | Johan/Alex/Jae |

## Youtube link
https://youtu.be/IPCtaopaBxY
