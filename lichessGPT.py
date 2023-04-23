import requests
from bs4 import BeautifulSoup
import chess.pgn
import re
import io
import openai
import logging
from Builder import Builder
import sys


def sliceSize(soup):
    all_tables = soup.find_all('div', {'class': 'pgn'})
    output = str(all_tables[0].text).splitlines(0)[-1]
    pgn = io.StringIO(output)
    node = chess.pgn.read_game(pgn)
    data = node.headers
    data["moves"] = []
    while node.variations:
        next_node = node.variation(0)
        data["moves"].append(
            re.sub("\{.*?\}", "", node.board().san(next_node.move)))
        node = next_node
    moveSize = len(data["moves"])
    sliceSize = moveSize + 1
    print("move_len: " + str(sliceSize))
    return getQueryList(moveSize, data)


def getQueryList(moveSize, data):
    qb = Builder(collection=None)

    for i in range(moveSize):
        qb.field("moves." + str(i) + "").equals(data["moves"][i])
    qb.field("my_side").equals(my_side)
    print(qb.get_query_list())
    return qb.get_query_list()


log = logging.getLogger().error

if len(sys.argv) > 2:

    id = sys.argv[1]
    my_side = sys.argv[2]

    r = requests.get('https://lichess.org/' + id + '')

    soup = BeautifulSoup(r.content, 'html.parser')

    jsonBuild = str(sliceSize(soup))

    completion = openai.ChatCompletion.create(model="gpt-3.5-turbo",
                                              messages=[{"role": "user", "content": jsonBuild + "next move"}])

    print(completion.choices[0].message.content)

else:
    print("lichess id or your side not entered")
