from pyspark.sql.functions import *

class SparkAppWritter:
    """
    Creates and build spark app
    """

    def __init__(self, syntax_tree) -> None:
        self.spark_app=SparkApp(syntax_tree["table"])
        self.syntax_tree=syntax_tree

    def write(self):
        # filter step
        if "filter" in self.syntax_tree:
            for item in self.syntax_tree["filter"]:
                alias = item[1][1]
                operation = item[1][0]+item[0]+'"'+item[2][0]+'"'
                self.spark_app.add_filter(alias, operation)

        # join
        if "join" in self.syntax_tree:
            for item in self.syntax_tree["join"]:
                alias1=item[1][1]
                alias2=item[2][1]
                operation=(item[0],(item[1][0],item[2][0]))
                self.spark_app.add_join(alias1, alias2, operation)

        # projection
        self.spark_app.add_projection(
            tuple([col[0] for col in self.syntax_tree["projection"]])
        )

        # group
        if "group_by" in self.syntax_tree:
            self.spark_app.add_group(
                (tuple(self.syntax_tree["group_by"]),
                    self.syntax_tree["window"]
                )
            )

        # agg
        if "aggregation" in self.syntax_tree:
            self.spark_app.add_agg(
                tuple(self.syntax_tree["aggregation"])
            )

        # having
        if "having" in self.syntax_tree:
            self.spark_app.add_having(
                self.syntax_tree["having"]
            )


        
        return self.spark_app

class Tree(object):
    def __init__(self, data):
        self.child = []
        self.data = data

def unary_operation(df_in, instruction):

    df_out=None

    if instruction[0] == "projection":
        print("step: projection", list(instruction[1]))
        df_out = df_in.select(list(instruction[1])+["data_node_timestampUTC"])

    if instruction[0] == "filter":
        print("step: filter", instruction[1], instruction[2])
        df_out = df_in.filter(instruction[2])

    if instruction[0] == "group":
        print("step: group ", "columns:", instruction[1][0], "window: ",instruction[1][1])
        group_list = list(instruction[1][0])
        group_list = group_list + [
            window(
                col("data_node_timestampUTC"),
                "2 minutes",
                "1 minutes"
                # instruction[1][1][0],
                # instruction[1][1][1],
            )
        ]
        df_out = df_in.groupBy(group_list)

    if instruction[0] == "agg":
        print("step: agg ", instruction[1])
        df_out = df_in.agg(
            dict([(item[1], item[0]) for item in instruction[1]])
        )

    return df_out
    

def binary_operation(df1_in, df2_in, instruction):
    if instruction[0] == "join":
        expression = "{}(a.{},b.{})".format(instruction[1][0],\
            instruction[1][1][0], instruction[1][1][1])
        print("step: join ", instruction[1])
        df_out = df1_in.alias("a").join(df2_in.alias("b"), \
            expr(expression))
    return df_out


class SparkApp():
    
    def __init__(self, tables) -> None:
        self.root = Tree("root")
        self.aliases = {}
        for table_name, alias in tables:
            self.root.child.append(Tree(("load",table_name)))
            self.aliases[alias] = table_name
        

    def add_filter(self, alias, node_operation):
        node = Tree(("filter",self.aliases[alias], node_operation))
        for i in range(0,len(self.root.child)):
            child = self.root.child[i]
            if child.data[1] == self.aliases[alias]:
                tmp_child = child
                node.child = [tmp_child]
                self.root.child[i] = node
                break

    def add_join(self, alias1, alias2, node_operation):
        root_list = []
        for i in range(0,len(self.root.child)):
            child = self.root.child[i]
            if child.data[1] == self.aliases[alias1]:
                child1 = child
            elif child.data[1] == self.aliases[alias2]:
                child2 = child
            else:
                root_list.append(child)

        node =( Tree((("join", node_operation, 
            (self.aliases[alias1],
            self.aliases[alias2]))
        )))
        node.child = [child1,child2]

        self.root.child = root_list + [node] 


    def add_projection(self,columns):
        tmp_node = self.root.child
        node = Tree(("projection",columns))
        node.child = tmp_node
        self.root.child = [node]

    def add_group(self, group):
        tmp_node = self.root.child
        node = Tree(("group",group))
        node.child = tmp_node
        self.root.child = [node]

    def add_agg(self, agg):
        tmp_node = self.root.child
        node = Tree(("agg",agg))
        node.child = tmp_node
        self.root.child = [node]

    def add_having(self, condition):
        tmp_node = self.root.child
        node = Tree(("having",condition))
        node.child = tmp_node
        self.root.child = [node]

    def add_sort():
        pass

    def get_tables(self):
        return list(self.aliases.values())
        

    def print(self):
        printer = []
        node = self.root
        def loop(node):
            if node.child != []:
                for child in node.child:
                    printer.append(child.data)
                    loop(child)
        loop(node)
        return printer

    def get_dataframe(self, df_dict):

        def loop(node):
            if len(node.child) == 2:
                # join node, run operation on 2 child
                print("BINARY")
                child1 = node.child[0]
                child2 = node.child[1]
                df = binary_operation(loop(child1), loop(child2), node.data) 
                return df
            
            if len(node.child) == 1:
                # run operation on 1 child
                print("UNARY")
                child = node.child[0]
                df = unary_operation(loop(child), node.data)
                return df            
            
            else:
                # load node, return dataframe
                print("LOAD")
                df = df_dict[node.data[1]]
                return df

        
        return loop(self.root.child[0])



