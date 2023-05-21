import scrapy
import pandas as pd
import numpy as np

class PostsSpider(scrapy.Spider):
    name = "posts"
    page = 2
    start_urls = [
        "https://stackoverflow.com/questions?tab=frequent&page=2"
    ]
    dataPoints = 0
    fileCounter = 1
    links = []
    titles = []
    questions = []
    answers = []

    def parse(self, response):
        print("Inside parsing", response)
        question_links = response.css('div.flush-left div.s-post-summary.js-post-summary '
                                      'h3.s-post-summary--content-title a::attr(href)').getall()
        PostsSpider.links.extend(question_links)

        question_titles = response.css('div.flush-left div.s-post-summary.js-post-summary '
                                       'h3.s-post-summary--content-title a::text').getall()
        PostsSpider.titles.extend(question_titles)

        for link in question_links:
            print("Going For --> ",  link)
            yield response.follow(link, callback=self.parse_content)

        # if self.dataPoints >= 5:
        #     data = pd.DataFrame(data=np.array([PostsSpider.links, PostsSpider.titles, PostsSpider.questions, PostsSpider.answers]).T,
        #                         columns=['post_link', 'post_title', 'post_question', 'post_answer'])
        #     data.to_pickle('stackoverflow-' + str(self.fileCounter) + '.pkl')
        #     PostsSpider.fileCounter += 1
        #     PostsSpider.dataPoints = 0
        #     PostsSpider.links.clear()
        #     PostsSpider.titles.clear()
        #     PostsSpider.questions.clear()
        #     PostsSpider.answers.clear()

        if PostsSpider.page < 52:
            PostsSpider.page += 1
            yield response.follow("https://stackoverflow.com/questions?tab=frequent&page=" + str(PostsSpider.page),
                                  callback=self.parse)

    def parse_content(self, response):
        print("Inside content parsing", PostsSpider.dataPoints)

        PostsSpider.dataPoints += 1

        post_question = " ".join(response.css('div.question.js-question div.post-layout div.s-prose.js-post-body '
                                              'p::text').getall())
        post_answer = " ".join(response.css('div.answer.js-answer.accepted-answer.js-accepted-answer '
                                            'div.answercell.post-layout--right div.s-prose.js-post-body '
                                            'p::text').getall())
        # print(post_question)
        PostsSpider.questions.append(post_question)
        PostsSpider.answers.append(post_answer)

    def closed(self, reason):
        data = pd.DataFrame(data=np.array([PostsSpider.links, PostsSpider.titles, PostsSpider.questions, PostsSpider.answers]).T,
                            columns=['post_link', 'post_title', 'post_question', 'post_answer'])
        data.to_pickle('stackoverflow-' + str(PostsSpider.fileCounter) + '.pkl')
