####################
#
#   Parser for the input query.
#   
#


import sqlparse
import re

from src.utils import get_logger


class Tree(object):
    def __init__(self, data):
        self.child = []
        self.data = data

class QueryTree():
    def __init__(self):
        self.elements={}
    def add_element(self, element):
        self.elements.update(element)

def where_parser(input):

    x = re.findall("\w*\(\w.\w* *, *\w.\w*\)|\w.\w*[=<>]\w*", input)

    result={}
    for item in x:
        #1
        x = re.search(
            "(\w*)\((\w).(\w*) *, *(\w).(\w*)\)", item
        )
        if x != None:
            matched = (x.group(1),(x.group(3),x.group(2)),(x.group(5),x.group(4)))
            if "join" not in result.keys():
                result["join"]=[]
            result["join"].append(matched)
        else:
            x = re.search(
                "(\w).(\w*)([=<>])(\w*)", item
            )
            matched = (x.group(3),(x.group(2),x.group(1)),(x.group(4),))
            if "filter" not in result.keys():
                result["filter"]=[]
            result["filter"].append(matched)

    return result

class Parser:

    stack = [
        ("Token.Keyword.DML", "SELECT"),
        ("None", "X", "columns"),
        ("Token.Keyword", "FROM"),
        ("None", "X", "from"),
        ("None", "X", "where"),
        ("Token.Keyword", "GROUP BY"),
        ("None", "X", "group"),
        ("Token.Keyword", "WINDOW"),
        ("None", "X", "window"),
    ]

    # define nodes
    select_node = Tree(("Token.Keyword.DML", "SELECT"))
    column_node = Tree(("None", "X", "columns"))
    from_token_node = Tree(("Token.Keyword", "FROM"))
    from_node = Tree(("None", "X", "from"))
    where_node = Tree(("None", "X", "where"))
    groupby_token_node = Tree(("Token.Keyword", "GROUP BY"))
    groupby_node = Tree(("None", "X", "group"))
    window_token_node = Tree(("Token.Keyword", "WINDOW"))
    window_node = Tree(("None", "X", "window"))
    having_token_node = Tree(("Token.Keyword", "HAVING"))
    having_node = Tree(("None", "X", "having"))

    # Build tree
    select_node.child = [column_node]
    column_node.child = [from_token_node]
    from_token_node.child = [from_node]
    from_node.child = [where_node, groupby_token_node]
    where_node.child = [groupby_token_node, window_token_node]
    groupby_token_node.child = [groupby_node]
    groupby_node.child = [window_token_node, having_token_node]
    window_token_node.child = [window_node]
    window_node.child = [having_token_node]
    having_token_node.child = [having_node]

    def __init__(self, query, logger=None):
        self.query_parsed = sqlparse.parse(query)
        if logger != None:
            self.logger = logger
        else:
            self.logger = get_logger("query_parser")
        self.tokens = self.query_parsed[0].tokens

    def scrap(self):
        self.parse_result = dict()
        i = 0
        nodes = [self.select_node]
        while i < len(self.tokens):
            if (
                str(self.tokens[i].ttype) == "Token.Text.Whitespace"
                or str(self.tokens[i].ttype) == "Token.Text.Whitespace.Newline"
            ):
                i += 1
            else:
                well_parsed = False
                for node in nodes:
                    if str(self.tokens[i].ttype) == node.data[0]:
                        if node.data[1] != "X":
                            if node.data[1] == self.tokens[i].value:
                                well_parsed = True
                                if node.child:
                                    nodes = node.child
                                break
                            else:
                                continue
                        elif node.data[1] == "X":
                            self.parse_result[node.data[2]] = self.tokens[
                                i
                            ].value
                            well_parsed = True
                            if node.child:
                                nodes = node.child
                            break
                if not well_parsed:
                    print(
                        "Query error at token {} {}".format(i, self.tokens[i])
                    )
                    exit(1)
                i += 1
        for key in self.parse_result.keys():
            self.parse_result[key] = " ".join(
                self.parse_result[key].split()
            )  # remove multiple spaces for each component

    def build_query_tree(self):

        query_tree = QueryTree()
        
        # table 
        tables = self.parse_result["from"].split(",")
        table_list=[]
        for table in tables:
            table_name, alias = table.split("AS")
            table_list.append((table_name.strip(), alias.strip()))
        query_tree.add_element({"table":table_list})

        # projection
        columns = [col.strip() for col in self.parse_result["columns"].split(",")]
        projection=[]
        for col in columns:
            x = re.search(
                " *(\w*) *\( *([\w|\.]+) *\) *", col
            )  # columns with aggregation
            if x:
                alias, column_name = x.group(2).split(".")
                projection.append((column_name, alias))
            else:
                alias, column_name = col.split(".")
                projection.append((column_name, alias))
        query_tree.add_element({"projection":projection})

        # window
        if "window" in self.parse_result.keys():
            window_length, sliding_length = self.parse_result["window"].split()
            query_tree.add_element({"window":(window_length, sliding_length)})

        # groupby columns
        if "group" in self.parse_result.keys():
            group_by_columns = [col.strip() for col in self.parse_result["group"].split(",")]
            query_tree.add_element({"group_by":group_by_columns})
        
        # agg columns
        columns = [col.strip() for col in self.parse_result["columns"].split(",")]
        aggregation=[]
        for col in columns:
            x = re.search(
                " *(\w*) *\( *([\w|\.]+) *\) *", col
            )  # columns with aggregation
            if x:
                alias, column_name = x.group(2).split(".")
                aggregation.append((x.group(1), column_name))
        if aggregation!=[]:
            query_tree.add_element({"aggregation":aggregation})

        # having
        if "having" in self.parse_result.keys():
            having = self.parse_result["having"].strip()
            query_tree.add_element({"having":having})

        #conditions
        if "where" in self.parse_result.keys():
            query_tree.add_element(where_parser(self.parse_result["where"]))


        return query_tree.elements

