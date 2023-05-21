import scrapy
import pandas as pd
import numpy as np

class PostsSpider(scrapy.Spider):
    name = "posts"
    page = 2
    start_urls = [
        "https://stackoverflow.com/questions?tab=frequent&page=2"
    ]

    def __init__(self, *args, **kwargs):
        super(PostsSpider, self).__init__(*args, **kwargs)
        self.dataPoints = 0
        self.fileCounter = 1
        self.links = []
        self.titles = []
        self.questions = []
        self.answers = []

    def parse(self, response):
        print("Inside parsing", response)
        question_links = response.css('div.flush-left div.s-post-summary.js-post-summary '
                                      'h3.s-post-summary--content-title a::attr(href)').getall()
        self.links.extend(question_links)

        question_titles = response.css('div.flush-left div.s-post-summary.js-post-summary '
                                       'h3.s-post-summary--content-title a::text').getall()
        self.titles.extend(question_titles)

        for link in question_links:
            print("Going For --> ",  link)
            yield response.follow(link, callback=self.parse_content)

        if self.dataPoints >= 5000:
            data = pd.DataFrame(data=np.array([self.links, self.titles, self.questions, self.answers]).T,
                                columns=['post_link', 'post_title', 'post_question', 'post_answer'])
            data.to_pickle('stackoverflow-' + str(self.fileCounter) + '.pkl')
            self.fileCounter += 1
            self.dataPoints = 0
            self.links.clear()
            self.titles.clear()
            self.questions.clear()
            self.answers.clear()

        if PostsSpider.page < 252:
            PostsSpider.page += 1
            yield response.follow("https://stackoverflow.com/questions?tab=frequent&page=" + str(PostsSpider.page),
                                  callback=self.parse)

    def parse_content(self, response):
        print("Inside content parsing", response)

        self.dataPoints += 1

        post_question = " ".join(response.css('div.question.js-question div.post-layout div.s-prose.js-post-body '
                                              'p::text').getall())
        post_answer = " ".join(response.css('div.answer.js-answer.accepted-answer.js-accepted-answer '
                                            'div.answercell.post-layout--right div.s-prose.js-post-body '
                                            'p::text').getall())
        self.questions.append(post_question)
        self.answers.append(post_answer)

    def closed(self, reason):
        data = pd.DataFrame(data=np.array([self.links, self.titles, self.questions, self.answers]).T,
                            columns=['post_link', 'post_title', 'post_question', 'post_answer'])
        data.to_pickle('stackoverflow-' + str(self.fileCounter) + '.pkl')
