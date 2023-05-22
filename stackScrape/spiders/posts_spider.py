import scrapy
import pandas as pd
import numpy as np

class PostsSpider(scrapy.Spider):
    name = "posts"
    page = 2
    start_urls = [
        "https://stackoverflow.com/questions?tab=frequent&page=2"
    ]
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

        print("Filecounter", PostsSpider.fileCounter)

        if len(PostsSpider.links) > (PostsSpider.fileCounter * 100) and len(PostsSpider.titles) > (PostsSpider.fileCounter * 100) and len(PostsSpider.questions) > (PostsSpider.fileCounter * 100) and len(PostsSpider.answers) > (PostsSpider.fileCounter * 100):
            start = 100 * (PostsSpider.fileCounter - 1)
            end = 100 * PostsSpider.fileCounter
            data = pd.DataFrame(data=np.array([PostsSpider.links[start:end], PostsSpider.titles[start:end], PostsSpider.questions[start:end], PostsSpider.answers[start:end]]).T,
                                columns=['post_link', 'post_title', 'post_question', 'post_answer'])
            data.to_pickle('stackoverflow-' + str(self.fileCounter) + '.pkl')
            PostsSpider.fileCounter += 1

        if PostsSpider.page < 600:
            PostsSpider.page += 1
            yield response.follow("https://stackoverflow.com/questions?tab=frequent&page=" + str(PostsSpider.page),
                                  callback=self.parse)

    def parse_content(self, response):

        post_question = " ".join(response.css('div.question.js-question div.post-layout div.s-prose.js-post-body '
                                              'p::text').getall())
        post_answer = " ".join(response.css('div.answer.js-answer.accepted-answer.js-accepted-answer '
                                            'div.answercell.post-layout--right div.s-prose.js-post-body '
                                            'p::text').getall())
        # print(post_question)
        PostsSpider.questions.append(post_question)
        PostsSpider.answers.append(post_answer)

    def closed(self, reason):
        start = 100 * (PostsSpider.fileCounter - 1)
        data = pd.DataFrame(data=np.array([PostsSpider.links[start:], PostsSpider.titles[start:], PostsSpider.questions[start:], PostsSpider.answers[start:]]).T,
                            columns=['post_link', 'post_title', 'post_question', 'post_answer'])
        data.to_pickle('stackoverflow-' + str(self.fileCounter) + '.pkl')
