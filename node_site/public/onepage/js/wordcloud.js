function build_wordcloud(by_type, target_selector) {
  $.getJSON(BASE_API_URL + "/wordcount/" + by_type, function(data) {
    var words = [];
    $.each(data, function (key, val) {
      words.push(
        {
          text: key,
          weight: val,
          html: {"class": "wordcloud-word"},
          handlers: {
            click: function (e) {
              alert(e);
            }
          },
          // afterWordRender: function(){}
        }
      );
    });

    $(target_selector).jQCloud(words, {
      delay: 50,
      autoResize: true,
      width: 500, height: 350
    });
  });
}

$(function() {
  let wordclouds = [['keywords', '#wordcloud-keywords'],['products', '#wordcloud-products']];
  for (let i = 0; i < wordclouds.length; i++) {
    build_wordcloud(wordclouds[i][0], wordclouds[i][1]);
  }
});