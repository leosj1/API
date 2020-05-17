# importing the requests library
import requests
import json
import urllib
import re

# # defining the api-endpoint
# API_ENDPOINT = "http://144.167.35.138:5000/clusterings"


# # data to be sent to api
# d = dict({'tracker_id': 7,'type': 'create'})

# # sending post request and saving response as response object
# r = requests.post(API_ENDPOINT, json=d)

# # extracting response text
# # pastebin_url = r.text
# # print("The pastebin URL is:%s"%pastebin_url)
# # print(data['tracker_id'])
# print(r)

# #Comments
# parsed_comment = Comments()
# parsed_comment['domain'] = self.allowed_domains[0]
# parsed_comment['url'] = 'testing'
# parsed_comment['comment_id'] = 'testing'
# parsed_comment['username'] = ""
# parsed_comment['user_id'] = ""
# parsed_comment['comment'] = 'testing'
# parsed_comment['comment_original'] = ""
# parsed_comment['links'] = get_links("<div class=\"entry-content clearfix\">\n<figure class=\"entry-thumbnail\">\n<img src=\"https://i1.wp.com/buffalochronicle.com/wp-content/uploads/2014/06/lead_option21.jpg?resize=678%2C381&amp;ssl=1\" alt=\"\" title=\"lead_option21\">\n</figure>\n<div class=\"mh-social-top\">\n<div class=\"mh-share-buttons clearfix\">\n\t<a class=\"mh-facebook\" href=\"#\" onclick=\"window.open('https://www.facebook.com/sharer.php?u=https%3A%2F%2Fbuffalochronicle.com%2F2014%2F06%2F29%2Fthe-pitchforks-are-coming%2F&amp;t=The+pitchforks+are+coming', 'facebookShare', 'width=626,height=436'); return false;\" title=\"Share on Facebook\">\n\t\t<span class=\"mh-share-button\"><i class=\"fa fa-facebook\"></i></span>\n\t</a>\n\t<a class=\"mh-twitter\" href=\"#\" onclick=\"window.open('https://twitter.com/share?text=The+pitchforks+are+coming:&amp;url=https%3A%2F%2Fbuffalochronicle.com%2F2014%2F06%2F29%2Fthe-pitchforks-are-coming%2F', 'twitterShare', 'width=626,height=436'); return false;\" title=\"Tweet This Post\">\n\t\t<span class=\"mh-share-button\"><i class=\"fa fa-twitter\"></i></span>\n\t</a>\n\t<a class=\"mh-linkedin\" href=\"#\" onclick=\"window.open('https://www.linkedin.com/shareArticle?mini=true&amp;url=https%3A%2F%2Fbuffalochronicle.com%2F2014%2F06%2F29%2Fthe-pitchforks-are-coming%2F&amp;source=', 'linkedinShare', 'width=626,height=436'); return false;\" title=\"Share on LinkedIn\">\n\t\t<span class=\"mh-share-button\"><i class=\"fa fa-linkedin\"></i></span>\n\t</a>\n\t<a class=\"mh-pinterest\" href=\"#\" onclick=\"window.open('https://pinterest.com/pin/create/button/?url=https%3A%2F%2Fbuffalochronicle.com%2F2014%2F06%2F29%2Fthe-pitchforks-are-coming%2F&amp;media=https://buffalochronicle.com/wp-content/uploads/2014/06/lead_option21.jpg&amp;description=The+pitchforks+are+coming', 'pinterestShare', 'width=750,height=350'); return false;\" title=\"Pin This Post\">\n\t\t<span class=\"mh-share-button\"><i class=\"fa fa-pinterest\"></i></span>\n\t</a>\n\t<a class=\"mh-googleplus\" href=\"#\" onclick=\"window.open('https://plusone.google.com/_/+1/confirm?hl=en-US&amp;url=https%3A%2F%2Fbuffalochronicle.com%2F2014%2F06%2F29%2Fthe-pitchforks-are-coming%2F', 'googleShare', 'width=626,height=436'); return false;\" title=\"Share on Google+\" target=\"_blank\">\n\t\t<span class=\"mh-share-button\"><i class=\"fa fa-google-plus\"></i></span>\n\t</a>\n\t<a class=\"mh-email\" href=\"mailto:?subject=The%20pitchforks%20are%20coming&amp;body=https%3A%2F%2Fbuffalochronicle.com%2F2014%2F06%2F29%2Fthe-pitchforks-are-coming%2F\" title=\"Send this article to a friend\" target=\"_blank\">\n\t\t<span class=\"mh-share-button\"><i class=\"fa fa-envelope-o\"></i></span>\n\t</a>\n\t<a class=\"mh-print\" href=\"javascript:window.print()\" title=\"Print this article\">\n\t\t<span class=\"mh-share-button\"><i class=\"fa fa-print\"></i></span>\n\t</a>\n</div></div>\n<p>In a featured article on <em><a title=\"The Pitchforks Are Coming\u2026 For Us Plutocrats\" href=\"http://www.politico.com/magazine/story/2014/06/the-pitchforks-are-coming-for-us-plutocrats-108014.html?hp=f3#.U7Ccho1OVdc\" target=\"_blank\">Politico</a>, </em>Seattle based venture capitalist Nick Hanauer warns that \u201cthe pitchforks are coming\u201d for him and his \u201cfellow plutocrats.\u201d</p>\n<p>He notes the stark wealth inequality in America: <em>\u201cAt the same time that people like you and me are thriving beyond the dreams of any plutocrats in history, the rest of the country\u2014the 99.99 percent\u2014is lagging far behind. The divide between the haves and have-nots is getting worse really, really fast. In 1980, the top 1 percent controlled\u00a0about 8 percent of U.S. national income. The bottom 50 percent shared about 18 percent. Today the top 1 percent share about 20 percent; the bottom 50 percent, just 12 percent.\u201d</em></p>\n<p>He warns: <em>\u201cIf we don\u2019t do something to fix the glaring inequities in this economy, the pitchforks are going to come for us. No society can sustain this kind of rising inequality. In fact, there is no example in human history where wealth accumulated like this and the pitchforks didn\u2019t eventually come out. You show me a highly unequal society, and I will show you a police state. Or an uprising. There are no counterexamples. None. It\u2019s not if, it\u2019s when.\u201d</em></p>\n<p><em>\u201cMany of us think we\u2019re special because \u201cthis is America.\u201d We think we\u2019re immune to the same forces that started the Arab Spring\u2014or the French and Russian revolutions, for that matter. I know you fellow .01%ers tend to dismiss this kind of argument; I\u2019ve had many of you tell me to my face I\u2019m completely bonkers.\u201d</em></p>\n<p>The article comes exactly six months after Kleiner Perkins venture capitalist Tom Perkins penned a letter in the <a title=\"Progressive Kristallnacht Coming?\" href=\"http://online.wsj.com/news/articles/SB10001424052702304549504579316913982034286\" target=\"_blank\"><em>Wall Street Journal </em></a>warning of a \u201cprogressive kristallnacht\u201d that he sees looming for America\u2019s rich. He reflects on das Zeitgeist: <em>\u201cThis is a very dangerous drift in our American thinking. Kristallnacht was unthinkable in 1930; is its descendent \u201cprogressive\u201d radicalism unthinkable now?\u201d</em></p>\n<p>These are leading venture capitalists who have made their fortunes predicting what will happen in the future. Let\u2019s all hope that this time, they are very wrong.</p>\n<p>\u00a0</p>\n<div class=\"sharedaddy sd-sharing-enabled\"><div class=\"robots-nocontent sd-block sd-social sd-social-official sd-sharing\"><div class=\"sd-content\"><ul><li class=\"share-twitter\"><a href=\"https://twitter.com/share\" class=\"twitter-share-button\" data-url=\"https://buffalochronicle.com/2014/06/29/the-pitchforks-are-coming/\" data-text=\"The pitchforks are coming \" data-via=\"BnChronicle\">Tweet</a></li><li class=\"share-facebook\"><div class=\"fb-share-button\" data-href=\"https://buffalochronicle.com/2014/06/29/the-pitchforks-are-coming/\" data-layout=\"button_count\"></div></li><li class=\"share-linkedin\"><div class=\"linkedin_button\"><script type=\"in/share\" data-url=\"https://buffalochronicle.com/2014/06/29/the-pitchforks-are-coming/\" data-counter=\"right\"></script></div></li><li class=\"share-reddit\"><div class=\"reddit_button\"><iframe src=\"https://www.reddit.com/static/button/button1.html?newwindow=true&amp;width=120&amp;url=https%3A%2F%2Fbuffalochronicle.com%2F2014%2F06%2F29%2Fthe-pitchforks-are-coming%2F&amp;title=The%20pitchforks%20are%20coming%20\" height=\"22\" width=\"120\" scrolling=\"no\" frameborder=\"0\"></iframe></div></li><li class=\"share-pinterest\"><div class=\"pinterest_button\"><a href=\"https://www.pinterest.com/pin/create/button/?url=https%3A%2F%2Fbuffalochronicle.com%2F2014%2F06%2F29%2Fthe-pitchforks-are-coming%2F&amp;media=https%3A%2F%2Fi1.wp.com%2Fbuffalochronicle.com%2Fwp-content%2Fuploads%2F2014%2F06%2Flead_option21.jpg%3Ffit%3D1200%252C651%26ssl%3D1&amp;description=The%20pitchforks%20are%20coming%20\" data-pin-do=\"buttonPin\" data-pin-config=\"beside\"><img src=\"https://i2.wp.com/assets.pinterest.com/images/pidgets/pinit_fg_en_rect_gray_20.png\" data-recalc-dims=\"1\" data-lazy-src=\"https://i2.wp.com/assets.pinterest.com/images/pidgets/pinit_fg_en_rect_gray_20.png?is-pending-load=1\" srcset=\"data:image/gif;base64,R0lGODlhAQABAIAAAAAAAP///yH5BAEAAAAALAAAAAABAAEAAAIBRAA7\" class=\" jetpack-lazy-image\"><noscript><img src=\"https://i2.wp.com/assets.pinterest.com/images/pidgets/pinit_fg_en_rect_gray_20.png\" data-recalc-dims=\"1\"></noscript></a></div></li><li class=\"share-end\"></ul></div></div></div><div class=\"sharedaddy sd-block sd-like jetpack-likes-widget-wrapper jetpack-likes-widget-unloaded\" id=\"like-post-wrapper-70000375-175-5eb58a1b46e05\" data-src=\"https://widgets.wp.com/likes/#blog_id=70000375&amp;post_id=175&amp;origin=buffalochronicle.com&amp;obj_id=70000375-175-5eb58a1b46e05\" data-name=\"like-post-frame-70000375-175-5eb58a1b46e05\"><h3 class=\"sd-title\">Like this:</h3><div class=\"likes-widget-placeholder post-likes-widget-placeholder\" style=\"height: 55px;\"><span class=\"button\"><span>Like</span></span> <span class=\"loading\">Loading...</span></div><span class=\"sd-text-color\"></span><a class=\"sd-link-color\"></a></div>\t</div>")
# parsed_comment['upvotes'] = None
# parsed_comment['downvotes'] = None
# parsed_comment['published_date'] = parse("May 4, 2020")
# parsed_comment['reply_count'] = 0
# parsed_comment['reply_to'] = 0
# yield parsed_comment

id_ = ""


def get_comments_data(comment_id):
    API_ENDPOINT = "https://public-api.wordpress.com/rest/v1/sites/70000375/comments/" + \
        str(comment_id)
    r = requests.get(url=API_ENDPOINT, params={})
    if r.status_code == 200:
        d = r.json()
    return d


def get_stats_data(post_id):
    API_ENDPOINT = "https://public-api.wordpress.com/rest/v1/sites/70000375/posts/" + \
        str(post_id)
    r = requests.get(url=API_ENDPOINT, params={})
    if r.status_code == 200:
        d = r.json()
    return d


def make_api_request(endpoint):
    r = requests.get(url=endpoint, params={})
    if r.status_code == 200:
        d = r.json()
    return d


pids__ = ['6935', '6924', '6886', '8286']


def build_batch(pids, siteid):
    api_url = 'https://public-api.wordpress.com/rest/v1/batch?'
    for pid in pids:
        appendee = f'&urls[]=/sites/{str(siteid)}/comments/{str(pid)}'
        api_url += appendee
    return api_url

# print('----',build_batch(pids__, 70000375))
# req = build_batch(pids__, 70000375)
# print(make_api_request(req))


if id_:
    data = get_comments_data(id_)
    # print('data--', data)

    parsed_comment = {}
    parsed_comment['url'] = data['URL']
    parsed_comment['username'] = data['author']['name']
    parsed_comment['user_id'] = data['author']['ID']
    parsed_comment['comment'] = data['raw_content']
    parsed_comment['comment_original'] = data['content']

    links = data['meta']['links']
    # parsed_comment['links'] = links
    parsed_comment['published_date'] = data['date']
    likes = requests.get(url=links['likes'], params={})

    if likes.status_code == 200:
        likes_data = likes.json()
        found_likes = likes_data['found']
        if found_likes:
            parsed_comment['upvotes'] = found_likes
        else:
            parsed_comment['upvotes'] = None
    else:
        print('error in comment like')
        parsed_comment['upvotes'] = None

    replies = requests.get(url=links['replies'], params={})
    if replies.status_code == 200:
        replies_data = replies.json()
        found_replies = replies_data['found']
        if found_replies:
            parsed_comment['reply_count'] = found_replies
        else:
            parsed_comment['reply_count'] = None
    else:
        print('error in comment replies')
        parsed_comment['reply_count'] = None
        # parsed_comment['username'] = data['nice_name']
        # parsed_comment['username'] = data['nice_name']
        # parsed_comment['username'] = data['nice_name']
        # parsed_comment['username'] = data['nice_name']
        # parsed_comment['username'] = data['nice_name']
        # parsed_comment['username'] = data['nice_name']
    print(parsed_comment)

pid = ""


# https://public-api.wordpress.com/rest/v1/batch?&urls[]=/sites/70000375/comments/6935&urls[]=/sites/70000375/comments/6924&urls[]=/sites/70000375/comments/6886&urls[]=/sites/70000375/comments/8286&urls[]=
posts = {}
if pid:
    stat = {}
    data = get_stats_data(pid)
    stat['url'] = data['URL']
    stat['likes'] = data['like_count']
    stat['views'] = None
    stat['comments'] = data['comment_count']
    posts['tags'] = dict(dict(data['tags']).keys())
    # print(stat)
    print(posts)

#Stats
# stat = Stats()
# stat['domain'] = self.domain
# stat['url'] = url
# stat['views'] = None
# stat['likes'] = None
# comments = get_comments(response.url)
# stat['comments'] =  comments['total'] if comments else None
# stats = get_stats_data()
body = {"query": "query CoralEmbedStream_Embed($assetId: ID, $assetUrl: String, $commentId: ID!, $hasComment: Boolean!, $excludeIgnored: Boolean, $sortBy: SORT_COMMENTS_BY!, $sortOrder: SORT_ORDER!) {\n  me {\n    id\n    state {\n      status {\n        username {\n          status\n          __typename\n        }\n        banned {\n          status\n          __typename\n        }\n        suspension {\n          until\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  asset(id: $assetId, url: $assetUrl) {\n    ...CoralEmbedStream_Configure_asset\n    ...CoralEmbedStream_Stream_asset\n    ...CoralEmbedStream_AutomaticAssetClosure_asset\n    __typename\n  }\n  ...CoralEmbedStream_Stream_root\n  ...CoralEmbedStream_Configure_root\n}\n\nfragment CoralEmbedStream_Stream_root on RootQuery {\n  me {\n    state {\n      status {\n        username {\n          status\n          __typename\n        }\n        banned {\n          status\n          __typename\n        }\n        suspension {\n          until\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    ignoredUsers {\n      id\n      __typename\n    }\n    role\n    __typename\n  }\n  settings {\n    organizationName\n    __typename\n  }\n  ...TalkSlot_StreamFilter_root\n  ...CoralEmbedStream_Comment_root\n  __typename\n}\n\nfragment CoralEmbedStream_Comment_root on RootQuery {\n  me {\n    ignoredUsers {\n      id\n      __typename\n    }\n    __typename\n  }\n  ...TalkSlot_CommentInfoBar_root\n  ...TalkSlot_CommentAuthorName_root\n  ...TalkEmbedStream_DraftArea_root\n  ...TalkEmbedStream_DraftArea_root\n  __typename\n}\n\nfragment TalkEmbedStream_DraftArea_root on RootQuery {\n  __typename\n}\n\nfragment CoralEmbedStream_Stream_asset on Asset {\n  comment(id: $commentId) @include(if: $hasComment) {\n    ...CoralEmbedStream_Stream_comment\n    parent {\n      ...CoralEmbedStream_Stream_singleComment\n      parent {\n        ...CoralEmbedStream_Stream_singleComment\n        parent {\n          ...CoralEmbedStream_Stream_singleComment\n          parent {\n            ...CoralEmbedStream_Stream_singleComment\n            parent {\n              ...CoralEmbedStream_Stream_singleComment\n              __typename\n            }\n            __typename\n          }\n          __typename\n        }\n        __typename\n      }\n      __typename\n    }\n    __typename\n  }\n  id\n  title\n  url\n  isClosed\n  created_at\n  settings {\n    moderation\n    infoBoxEnable\n    infoBoxContent\n    premodLinksEnable\n    questionBoxEnable\n    questionBoxContent\n    questionBoxIcon\n    closedTimeout\n    closedMessage\n    disableCommenting\n    disableCommentingMessage\n    charCountEnable\n    charCount\n    requireEmailConfirmation\n    __typename\n  }\n  totalCommentCount @skip(if: $hasComment)\n  comments(query: {limit: 50000, excludeIgnored: $excludeIgnored, sortOrder: $sortOrder, sortBy: $sortBy}) @skip(if: $hasComment) {\n    nodes {\n      ...CoralEmbedStream_Stream_comment\n      __typename\n    }\n    hasNextPage\n    startCursor\n    endCursor\n    __typename\n  }\n  ...TalkSlot_StreamFilter_asset\n  ...CoralEmbedStream_Comment_asset\n  __typename\n}\n\nfragment CoralEmbedStream_Comment_asset on Asset {\n  __typename\n  id\n  ...TalkSlot_CommentInfoBar_asset\n  ...TalkSlot_CommentReactions_asset\n  ...TalkSlot_CommentAuthorName_asset\n}\n\nfragment CoralEmbedStream_Stream_comment on Comment {\n  id\n  status\n  user {\n    id\n    __typename\n  }\n  ...CoralEmbedStream_Comment_comment\n  __typename\n}\n\nfragment CoralEmbedStream_Comment_comment on Comment {\n  ...CoralEmbedStream_Comment_SingleComment\n  replies(query: {limit : 50000, excludeIgnored: $excludeIgnored}) {\n    nodes {\n      ...CoralEmbedStream_Comment_SingleComment\n      replies(query: {limit : 50000, excludeIgnored: $excludeIgnored}) {\n        nodes {\n          ...CoralEmbedStream_Comment_SingleComment\n          replies(query: {limit : 50000, excludeIgnored: $excludeIgnored}) {\n            nodes {\n              ...CoralEmbedStream_Comment_SingleComment\n              replies(query: {limit : 50000, excludeIgnored: $excludeIgnored}) {\n                nodes {\n                  ...CoralEmbedStream_Comment_SingleComment\n                  replies(query: {limit : 50000, excludeIgnored: $excludeIgnored}) {\n                    nodes {\n                      ...CoralEmbedStream_Comment_SingleComment\n                      __typename\n                    }\n                    hasNextPage\n                    startCursor\n                    endCursor\n                    __typename\n                  }\n                  __typename\n                }\n                hasNextPage\n                startCursor\n                endCursor\n                __typename\n              }\n              __typename\n            }\n            hasNextPage\n            startCursor\n            endCursor\n            __typename\n          }\n          __typename\n        }\n        hasNextPage\n        startCursor\n        endCursor\n        __typename\n      }\n      __typename\n    }\n    hasNextPage\n    startCursor\n    endCursor\n    __typename\n  }\n  __typename\n}\n\nfragment CoralEmbedStream_Comment_SingleComment on Comment {\n  id\n  body\n  created_at\n  status\n  replyCount\n  tags {\n    tag {\n      name\n      __typename\n    }\n    __typename\n  }\n  user {\n    id\n    username\n    __typename\n  }\n  status_history {\n    type\n    __typename\n  }\n  action_summaries {\n    __typename\n    count\n    current_user {\n      id\n      __typename\n    }\n  }\n  editing {\n    edited\n    editableUntil\n    __typename\n  }\n  ...TalkSlot_CommentInfoBar_comment\n  ...TalkSlot_CommentReactions_comment\n  ...TalkSlot_CommentAvatar_comment\n  ...TalkSlot_CommentAuthorName_comment\n  ...TalkSlot_CommentContent_comment\n  ...TalkEmbedStream_DraftArea_comment\n  ...TalkEmbedStream_DraftArea_comment\n  __typename\n}\n\nfragment TalkEmbedStream_DraftArea_comment on Comment {\n  __typename\n  ...TalkSlot_DraftArea_comment\n}\n\nfragment CoralEmbedStream_Stream_singleComment on Comment {\n  id\n  status\n  user {\n    id\n    __typename\n  }\n  ...CoralEmbedStream_Comment_SingleComment\n  __typename\n}\n\nfragment CoralEmbedStream_Configure_root on RootQuery {\n  __typename\n  ...CoralEmbedStream_Settings_root\n}\n\nfragment CoralEmbedStream_Settings_root on RootQuery {\n  __typename\n}\n\nfragment CoralEmbedStream_Configure_asset on Asset {\n  __typename\n  ...CoralEmbedStream_AssetStatusInfo_asset\n  ...CoralEmbedStream_Settings_asset\n}\n\nfragment CoralEmbedStream_AssetStatusInfo_asset on Asset {\n  id\n  closedAt\n  isClosed\n  __typename\n}\n\nfragment CoralEmbedStream_Settings_asset on Asset {\n  id\n  settings {\n    moderation\n    premodLinksEnable\n    questionBoxEnable\n    questionBoxIcon\n    questionBoxContent\n    __typename\n  }\n  __typename\n}\n\nfragment CoralEmbedStream_AutomaticAssetClosure_asset on Asset {\n  id\n  closedAt\n  __typename\n}\n\nfragment TalkSlot_StreamFilter_root on RootQuery {\n  ...TalkViewingOptions_ViewingOptions_root\n  __typename\n}\n\nfragment TalkViewingOptions_ViewingOptions_root on RootQuery {\n  __typename\n}\n\nfragment TalkSlot_CommentInfoBar_root on RootQuery {\n  ...TalkModerationActions_root\n  __typename\n}\n\nfragment TalkModerationActions_root on RootQuery {\n  me {\n    id\n    __typename\n  }\n  __typename\n}\n\nfragment TalkSlot_CommentAuthorName_root on RootQuery {\n  ...TalkAuthorMenu_AuthorName_root\n  __typename\n}\n\nfragment TalkAuthorMenu_AuthorName_root on RootQuery {\n  __typename\n  ...TalkSlot_AuthorMenuActions_root\n}\n\nfragment TalkSlot_StreamFilter_asset on Asset {\n  ...TalkViewingOptions_ViewingOptions_asset\n  __typename\n}\n\nfragment TalkViewingOptions_ViewingOptions_asset on Asset {\n  __typename\n}\n\nfragment TalkSlot_CommentInfoBar_asset on Asset {\n  ...TalkModerationActions_asset\n  ...TalkPermalink_Button_asset\n  __typename\n}\n\nfragment TalkModerationActions_asset on Asset {\n  id\n  __typename\n}\n\nfragment TalkPermalink_Button_asset on Asset {\n  url\n  __typename\n}\n\nfragment TalkSlot_CommentReactions_asset on Asset {\n  ...VoteButton_asset\n  __typename\n}\n\nfragment VoteButton_asset on Asset {\n  id\n  __typename\n}\n\nfragment TalkSlot_CommentAuthorName_asset on Asset {\n  ...TalkAuthorMenu_AuthorName_asset\n  __typename\n}\n\nfragment TalkAuthorMenu_AuthorName_asset on Asset {\n  __typename\n}\n\nfragment TalkSlot_CommentInfoBar_comment on Comment {\n  ...CollapseCommentButton_comment\n  ...TalkModerationActions_comment\n  ...TalkPermalink_Button_comment\n  ...TalkInfoBar_moveReportButton_Comment\n  ...TalkInfoBar_addEdiableClass_Comment\n  __typename\n}\n\nfragment CollapseCommentButton_comment on Comment {\n  id\n  replyCount\n  __typename\n}\n\nfragment TalkModerationActions_comment on Comment {\n  id\n  status\n  user {\n    id\n    __typename\n  }\n  tags {\n    tag {\n      name\n      __typename\n    }\n    __typename\n  }\n  __typename\n}\n\nfragment TalkPermalink_Button_comment on Comment {\n  id\n  __typename\n}\n\nfragment TalkInfoBar_moveReportButton_Comment on Comment {\n  id\n  __typename\n}\n\nfragment TalkInfoBar_addEdiableClass_Comment on Comment {\n  id\n  editing {\n    __typename\n    editableUntil\n  }\n  __typename\n}\n\nfragment TalkSlot_CommentReactions_comment on Comment {\n  ...TalkDisableDeepReplies_disableDeepReplies_Comment\n  ...VoteButton_comment\n  __typename\n}\n\nfragment TalkDisableDeepReplies_disableDeepReplies_Comment on Comment {\n  id\n  __typename\n}\n\nfragment VoteButton_comment on Comment {\n  id\n  action_summaries {\n    __typename\n    ... on UpvoteActionSummary {\n      count\n      current_user {\n        id\n        __typename\n      }\n      __typename\n    }\n    ... on DownvoteActionSummary {\n      count\n      current_user {\n        id\n        __typename\n      }\n      __typename\n    }\n  }\n  __typename\n}\n\nfragment TalkSlot_CommentAvatar_comment on Comment {\n  ...UserAvatar_comment\n  __typename\n}\n\nfragment UserAvatar_comment on Comment {\n  user {\n    avatar\n    __typename\n  }\n  __typename\n}\n\nfragment TalkSlot_CommentAuthorName_comment on Comment {\n  ...TalkAuthorMenu_AuthorName_comment\n  __typename\n}\n\nfragment TalkAuthorMenu_AuthorName_comment on Comment {\n  __typename\n  id\n  user {\n    username\n    __typename\n  }\n  ...TalkSlot_AuthorMenuActions_comment\n}\n\nfragment TalkSlot_CommentContent_comment on Comment {\n  ...TalkPluginRichText_CommentContent_comment\n  __typename\n}\n\nfragment TalkPluginRichText_CommentContent_comment on Comment {\n  body\n  richTextBody\n  __typename\n}\n\nfragment TalkSlot_DraftArea_comment on Comment {\n  ...TalkPluginRichText_Editor_comment\n  __typename\n}\n\nfragment TalkPluginRichText_Editor_comment on Comment {\n  body\n  richTextBody\n  __typename\n}\n\nfragment TalkSlot_AuthorMenuActions_root on RootQuery {\n  ...TalkIgnoreUser_IgnoreUserAction_root\n  __typename\n}\n\nfragment TalkIgnoreUser_IgnoreUserAction_root on RootQuery {\n  me {\n    id\n    __typename\n  }\n  __typename\n}\n\nfragment TalkSlot_AuthorMenuActions_comment on Comment {\n  ...TalkIgnoreUser_IgnoreUserAction_comment\n  ...TalkDrupalUserId_DrupalProfile_comment\n  __typename\n}\n\nfragment TalkIgnoreUser_IgnoreUserAction_comment on Comment {\n  user {\n    id\n    __typename\n  }\n  ...TalkIgnoreUser_IgnoreUserConfirmation_comment\n  __typename\n}\n\nfragment TalkIgnoreUser_IgnoreUserConfirmation_comment on Comment {\n  user {\n    id\n    username\n    __typename\n  }\n  __typename\n}\n\nfragment TalkDrupalUserId_DrupalProfile_comment on Comment {\n  user {\n    id\n    __typename\n  }\n  __typename\n}\n", "variables": {"assetId": "", "assetUrl": "https://www.zerohedge.com/geopolitical/second-wave-china-orders-partial-lockdown-border-city-seouls-newest-cluster-explodes", "commentId": "", "hasComment": False, "excludeIgnored": False, "sortBy": "CREATED_AT", "sortOrder": "DESC"}, "operationName": "CoralEmbedStream_Embed"}

# r = requests.post(url='https://talk.zerohedge.com/api/v1/graph/ql', json=body)
# endpoint = "https://www.facebook.com/plugins/comments.php?app_id=249643311490&channel=https%3A%2F%2Fstaticxx.facebook.com%2Fconnect%2Fxd_arbiter.php%3Fversion%3D46%23cb%3Df252110aaf68fe%26domain%3Dtorontosun.com%26origin%3Dhttps%253A%252F%252Ftorontosun.com%252Ff3d719544ed86e4%26relation%3Dparent.parent&container_width=1129&height=100&href=https%3A%2F%2Ftorontosun.com%2Fopinion%2Fcolumnists%2Flilley-ford-starts-reopening-but-not-fast-enough&locale=en_US&sdk=joey&width=1129"

# r = requests.get(url=endpoint, params={})
# # print(r)

# with open("sample.json", "w") as outfile:
#     json.dump(r.json(), outfile)


# https://www.facebook.com/plugins/comments.php?app_id=249643311490&channel=https%3A%2F%2Fstaticxx.facebook.com%2Fconnect%2Fxd_arbiter.php%3Fversion%3D46%23cb%3Df2b78156879da74%26domain%3Dtorontosun.com%26origin%3Dhttps%253A%252F%252Ftorontosun.com%252Ff1e62f813d5211c%26relation%3Dparent.parent&container_width=1129&height=100&href=https%3A%2F%2Ftorontosun.com%2Fopinion%2Fcolumnists%2Flilley-ford-starts-reopening-but-not-fast-enough&locale=en_US&sdk=joey&width=1129


# https://www.facebook.com/plugins/feedback.php?app_id=249643311490&href=https%3A%2F%2Ftorontosun.com%2Fopinion%2Fcolumnists%2Flilley-ford-starts-reopening-but-not-fast-enough

# comment_id = 3799983366738888


def get_comments(post_url, app_id):
    feed_url = urllib.parse.quote(post_url, safe='')
    url = f"https://www.facebook.com/plugins/feedback.php?app_id={app_id}&href={feed_url}"

    #Sending request for fb Iframe
    r = requests.get(url)

    commentcount = 0
    s = str(r.text)
    if 'instances' in s:
        res = re.search('({\"instances\".+}\);)', s).group(0)
        if str(res)[-2:] == ');':
            commentcount = json.loads(str(res)[:-2])['require'][2][3][0]['props']['meta']['totalCount']

    print('commentcount--',commentcount)

    #Getting the main comment id for the article
    pos = r.text.find("commentIDs") + len("commentIDs") + 4
    comment_id = r.text[pos:pos+16]



    # comment_id_ = r.text[pos:pos+40]
    # if '_' in comment_id_:
    #     comment_id = comment_id_.split('_')[0]
    # else:
    #     comment_id = comment_id_

    #Sending the API request for comments, loading data
    url = f'https://www.facebook.com/plugins/comments/async/{comment_id}/pager/social/'

    print('commentid ---', comment_id)

    form_data = {
        'limit': 500000,
        '__a': 1}

    session = requests.Session()
    r = session.post(url, data=form_data)

    with open("sample2.txt", "w") as outfile:
        outfile.write(str(r.content))


    error = False
    if len(r.content) <= 13:
        #No comments on the page
        return None, None
    else:
        try:
            data = json.loads(r.content[9:])
        except Exception as e:
            if 'Expecting value:' in str(e):
                error = True
            # print('---err',e)
    

    if error and commentcount == 1:  
        
                # comment_array = 
                comments = json.loads(str(res)[:-2])['require'][2][3][0]['props']['comments']['idMap']
                print('---', comments)
    else:
        print('err----',error)
        comments = data['payload']['idMap']
                # 
            # 
            # json.loads(d)

    #Parsing the comments
    comment_list = []
    author_list = []
    
    for item in comments:
        # Checking if it's a comment, comments have a longer id than just the comment id, checking that is comment type
        if comment_id in item and comment_id != item and comments[item]['type'] == 'comment':
            comment_list.append(comments[item])
            #Sending additional requests for replies
            if 'public_replies' in comments[item] and 'afterCursor' in comments[item]['public_replies']:
                replies, authors = get_replies(item, comments[item]['public_replies']['afterCursor'])
                comment_list += replies
                author_list += authors
        else:
            author_list.append(comments[item])

    return comment_list, author_list


def get_replies(comment_id, after_cursor):
    #Sending replies request, loading data
    url = f'https://www.facebook.com/plugins/comments/async/comment/{comment_id}/pager/'
    form_data = {
    'after_cursor': after_cursor,
    'limit': 500000,
    '__a': 1
    }
    session = requests.Session()
    r = session.post(url, data=form_data)
    data = json.loads(r.content[9:])
    # print('r.content---------',r.content)

    

    #Resettign comment id, parsing replies
    comment_list = []
    author_list = []
    original_id = comment_id
    comment_id = comment_id[:16]
    comments = data['payload']['idMap']
    for item in comments:
        # Checking if it's a comment, comments have a longer id than just the comment id, checking that it is comment type
        if comment_id in item and comment_id != item and comments[item]['type'] == 'comment':
            comment_list.append(comments[item])
            #Sending additional requests for replies
            if 'public_replies' in comments[item] and 'afterCursor' in comments[item]['public_replies']:
                replies, authors = get_replies(item, comments[item]['public_replies']['afterCursor'])
                comment_list += replies
                author_list += authors
        elif item != original_id:
            author_list.append(comments[item])
    return comment_list, author_list


# url__ = 'https://torontosun.com/sports/baseball/toronto-blue-jays/astros-blast-off-with-another-victory-over-blue-jays'

# url__ = "https://torontosun.com/news/national/federal-covid-19-wage-subsidy-to-last-through-summer-trudeau-says/wcm/b38e4710-92ba-48cc-b256-b6c1bd1efd9c"
# url__ = "https://torontosun.com/sports/soccer/mls/toronto-fc/the-nightmare-continues-for-fading-toronto-fc"
url__ = "https://torontosun.com/opinion/columnists/parkin-ndp-outsider-aims-to-pry-open-ottawas-secret-ways"

comments = get_comments(url__, 162111247988300)

with open("comment_list.json", "w") as outfile:
    json.dump(comments[0], outfile)

with open("author_list.json", "w") as outfile:
    json.dump(comments[1], outfile)

if comments[0]:
    print('number of comments', len(comments[0]))
    print('number of authors', len(comments[1]))

# print(get_comments(url__)[0])

# https://www.facebook.com/plugins/feedback.php?
# app_id=162111247988300&
# channel=https%3A%2F%2Fstaticxx.facebook.com%2Fconnect%2Fxd_arbiter.php%3Fversion%3D46%23cb%3Df2eedbbd2b86f14%26domain%3D
# www.buzzfeednews.com%26origin%3Dhttps%253A%252F%252F
# www.buzzfeednews.com%252Ff2cdc29d284cb9%26relation%3Dparent.parent&container_width=443&height=100&
# href=https%3A%2F%2Fwww.buzzfeed.com%2Fskbaer%2Fcoronavirus-beauty-industry-bailout-hairstylists&
# locale=en_US&mobile=true&sdk=joey&version=v2.9


# print(urllib.parse.quote('https://www.buzzfeednews.com/article/aramroston/in-an-unmarked-grave-a-baby-who-died-on-for-profit-foster-co', safe=''))
