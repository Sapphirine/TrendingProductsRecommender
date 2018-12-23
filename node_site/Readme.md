## Execution
run by executing: $npm run watch or $nodemon start.js


## Update Dashboard
#To Update Data for Pie Chart categories, change the dataset variable in index.html:
function dsPieChart(){

        var dataset = [
            {category: "Toys", measure: 0.30},
              {category: "Games", measure: 0.25},
              {category: "Electronics", measure: 0.15},
              {category: "Snacks", measure: 0.15}
              ]
              ;
			  
			  
#To Update Data for Bar Chart keywords, change the datasetBarChart variable in index.html:
function dsPieChart(){

var datasetBarChart = [
{ group: "All", category: "Oranges", measure: 63850.4963 }, 
{ group: "All", category: "Apples", measure: 78258.0845 }, 
{ group: "All", category: "Grapes", measure: 60610.2355 }, 
...
]
;

## Update Wordcloud
#To update the data for the Wordcloud drop down, update the text-select section in index.html:
<select id="text-select">
              <option value="snack">Snacks</option>
              <option value="toys">Toys</option>
              <option value="games">Games</option>
              <option value="hice">HICE</option>
            </select>
			
To update file locations, bubble keywords and others, update the vis.coffee file in ./coffee:
texts = [
  {key:"snack",file:"snacks.csv",name:"Snacks"}
  {key:"toys",file:"toys.csv",name:"Toys"}
  {key:"games",file:"games.csv",name:"Games"}
  {key:"hice",file:"hice.csv",name:"HICE"}
]
			
			
# API Endpoints
http://35.211.101.139:8081/wordcount/keywords
http://35.211.101.139:8081/get_tweets/3/favorite_count/DESC/electronics
url format is http://35.211.101.139:8081/get_tweets/<number of tweets>/<sort by field>/<sort order>/<category>
so http://35.211.101.139:8081/get_tweets/10/retweet_count/desc/video_games would return the top 10 tweets sorted by retweet_count in descending order within the category "video_games"
The count field needs an int, so asking for default means setting it to 0. So http://35.211.101.139:8081/get_tweets/0/default/asc/default or http://35.211.101.139:8081/get_tweets/0/default/asc (since you can leave off trailing params if you want them to be defaults) would return the top 20 tweets sorted by favorite_count in ASCending order across all categories
