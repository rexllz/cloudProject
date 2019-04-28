#=============================================================================
#
# Author: Arby Zhang
#
# email : elegance.zf@gmail.com
#
# Filename: imooc_name.py
#
#!/usr/bin/env python3
# -*-encoding=utf8 -*-
#
#=============================================================================

import urllib.request
import bs4
import re

def html_downloader(url):
    try:
        request = urllib.request.Request(url)
        request.add_header('user-agent', 'Mozilla/5.0')
        response = urllib.request.urlopen(request)

        return response.read()
    except:
        print('html_downloader failed')
        exit(0)

def get_courses_list_entry(num, url_prefix):

    num_pages = num
    url = []

    for i in range(1, num_pages+1):
        url.append(url_prefix+str(i))
    
    return url

def free_courses_list_html_parser(urls):

    courses_info = []
    learn_urls = []


    for url in urls:

#        print(url)

        html_cnt = html_downloader(url)

        soup = bs4.BeautifulSoup(html_cnt, 'html.parser', from_encoding='utf-8')

        soup = soup.select('.course-card')

        for item in soup:
            learn_url = item['href']
            learn_name = item.select('.course-card-name')[0].string
            learn_label = item.select('.course-label')[0].label.string
            courses_info.append((learn_url, learn_name, learn_label))
            learn_urls.append(learn_url)

    return courses_info, learn_urls

def free_video_list_html_parser(urls):

    videos_info = []

    for url in urls:

#        print("url:", url)

        html_cnt = html_downloader('http://www.imooc.com' + url)

        soup = bs4.BeautifulSoup(html_cnt, 'html.parser', from_encoding='utf-8')

        soup = soup.select('.J-media-item')

        for item in soup:
            video_url = item['href']
            video_name = re.sub('\s','', item.text)[:-4]
            videos_info.append((url, video_url, video_name))

    return videos_info

# Deprecated
def cost_courses_list_html_parser(urls):

    courses_info = []
    courses_url = []

    for url in urls:
        html_cnt = html_downloader(url)

        soup = bs4.BeautifulSoup(html_cnt, 'html.parser', from_encoding='utf-8')

        soup = soup.select('.shizhan-course-wrap')

        for item in soup:
            course_url = item.a['href']
            course_name = item.p.string
            courses_info.append((course_url, course_name))
            courses_url.append(course_url)

    return courses_info, courses_url


if __name__=='__main__':
    free_courses_url_prefix = 'http://www.imooc.com/course/list?page='
    free_courses_num_pages = 30
    courses_info, courses_url = free_courses_list_html_parser(get_courses_list_entry(free_courses_num_pages, free_courses_url_prefix))

    for course_info in courses_info:
        print(course_info[0]+', '+course_info[1]+', '+course_info[2])

#    videos_info = free_video_list_html_parser(courses_url)

#    for video_info in videos_info:
#        print(video_info[0]+', '+video_info[1]+', '+video_info[2])

#    cost_courses_url_prefix = 'https://coding.imooc.com/?sort=0&unlearn=0&page='
#    cost_courses_num_pages = 1
#    
#    courses_info, courses_url = cost_courses_list_html_parser(get_courses_list_entry(cost_courses_num_pages, cost_courses_url_prefix))
#
#    print(courses_info, courses_url)
