from __future__ import annotations
import sqlite3
import pathlib
from copy import deepcopy
import re
import hashlib
from typing import Any, Dict, List, Optional, Tuple
import re
import networkx as nx
DB_PATH = "persistance_tracker.db"

def _init_db(conn: sqlite3.Connection):
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        type TEXT, -- tem_dat, tem_cal, tem_group, ins_dat, ins_cal, ins_group
        extra TEXT, -- JSON for any extra data
        UNIQUE(name, type)
    );
    CREATE TABLE IF NOT EXISTS edges (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first INTEGER,
        second INTEGER,
        FOREIGN KEY (first) REFERENCES nodes(id),
        FOREIGN KEY (second) REFERENCES nodes(id),
        UNIQUE(first, second)
    );

    """)
    conn.commit()


class TemplateGroup:
    """Wraps around a set of templates nodes and edges."""

    def __init__(self, database: Database):
        self.database = database
        self.graph: Optional[nx.DiGraph] = None # Stores the template as graph.
        self.hash : Optional[str] = None

    def register(self, name: str, command: str):
        """
        Given the name and the command of the template it creates template nodes
        What it does:
        - Extract inputs and outputs using regex
        - Create calculation nodes and all relevant data nodes. and insert into the database.
        """
        # extract inputs, outputs and the command
        inputs = re.findall(r'input\((.*?)\)', command)
        outputs = re.findall(r'output\((.*?)\)', command)

        # add the template calculation node. (add calculation node as json)
        self.database.cursor.execute(
            "INSERT OR IGNORE INTO nodes (name, type, extra) VALUES (?, ?, ?)",
            (name, "tem_cal", '{"command":' + f'"{command}"' + '}' )
        )
        
        # Insert inputs and outputs
        for inp in inputs:
            self.database.cursor.execute(
                "INSERT OR IGNORE INTO nodes (name, type) VALUES (?, ?)",
                (inp, "tem_dat")
            )
            # create edge from input to calculation
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES ((SELECT id FROM nodes WHERE name = ? AND type = 'tem_dat'),
                    (SELECT id FROM nodes WHERE name = ? AND type = 'tem_cal')
                )
                """,
                (inp, name)
            )
        for out in outputs:
            self.database.cursor.execute(
                "INSERT OR IGNORE INTO nodes (name, type) VALUES (?, ?)",
                (out, "tem_dat")
            )
            # create edge from calculation to output
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES ((SELECT id FROM nodes WHERE name = ? AND type = 'tem_cal'),
                    (SELECT id FROM nodes WHERE name = ? AND type = 'tem_dat')
                )
                """,
                (name, out)
            )
        
        # add to the graph (for in-memory representation and used to calculate the hash.)
        if self.graph is None:
            self.graph = nx.DiGraph()
        self.graph.add_node(name, type="tem_cal", command=command)
        for inp in inputs:
            self.graph.add_node(inp, type="tem_dat")
            self.graph.add_edge(inp, name)
        for out in outputs:
            self.graph.add_node(out, type="tem_dat")
            self.graph.add_edge(name, out)

    def construct_graph(self, hash: str):
        """Construct the graph from the database given the hash."""
        self.hash = hash
        if self.graph is not None:
            raise ValueError("Graph is already in memory. This function is for creating graph from the database. You are in a wrong mode.")
        
        self.graph = nx.DiGraph()
        # Query the database to reconstruct the graph
        self.database.cursor.execute(
            """
            SELECT n.id, n.name, n.type, json_extract(n.extra, '$.command') AS command
            FROM nodes n
            JOIN edges e ON n.id = e.second
            WHERE e.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            """,
            (self.hash,)
        )
        nodes = self.database.cursor.fetchall()
        node_dict = {}
        for node_id, name, typ, extra in nodes:
            self.graph.add_node(name, label = node_id, type = typ, command = extra)
            node_dict[node_id] = name
        # Get all edges between these nodes
        self.database.cursor.execute(
            """
            SELECT e.first, e.second
            FROM edges e
            WHERE e.first IN (
                SELECT n.id
                FROM nodes n
                JOIN edges e2 ON n.id = e2.second
                WHERE e2.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            )
            AND e.second IN (
                SELECT n.id
                FROM nodes n
                JOIN edges e2 ON n.id = e2.second
                WHERE e2.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            )
            """,
            (self.hash, self.hash)
        )
        edges = self.database.cursor.fetchall()
        for first, second in edges:
            self.graph.add_edge(node_dict[first], node_dict[second])

    def commit(self) -> str:
        """
        Commit the template group to the database.
        Add tem_group node and 
        """
        if self.graph is None:
            raise ValueError("In memory graph is not defined. Either it was never created or already committed.")

        # Create a unique hash for the template group
        group_hash = self._hash()
        # Insert the tem_group node
        self.database.cursor.execute(
            "INSERT OR IGNORE INTO nodes (name, type, extra) VALUES (?, ?, ?)",
            (group_hash, "tem_group", None)
        )
        # Insert edges between group and all template nodes
        for node in self.graph.nodes:
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES (
                    (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group'),
                    (SELECT id FROM nodes WHERE name = ?)
                )
                """,
                (group_hash, node)
            )

        self.database.conn.commit()
        self.graph = None
        self.hash = group_hash
        return group_hash

    def _hash(self) -> str:
        """Create a unique hash for the template group."""
        import hashlib
        # Create a sorted representation of the graph
        nodes = sorted(self.graph.nodes)
        edges = sorted((u, v) for u, v in self.graph.edges)
        representation = str(nodes) + str(edges)
        return hashlib.md5(representation.encode()).hexdigest()

    def graph_dot(self) -> str:
        """A method for testing before committing. To make sure the graph is correct."""
        return nx.nx_pydot.to_pydot(self.graph).to_string()

    def as_dot(self) -> str:
        """Reconstruct the graph from the database."""
        if self.hash is None:
            raise ValueError("Hash is not defined.")

        if self.graph is not None:
            raise ValueError("Graph is still in memory. Please commit first.")
        
        # Query the database to reconstruct the graph
        dot = "digraph G {\n"
        # Get all nodes in the group
        self.database.cursor.execute(
            """
            SELECT n.id, n.name, n.type, n.extra
            FROM nodes n
            JOIN edges e ON n.id = e.second
            WHERE e.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            """,
            (self.hash,)
        )
        nodes = self.database.cursor.fetchall()
        node_dict = {}
        for node_id, name, ntype, extra in nodes:
            label = f"{name}"

            dot += f'  {node_id} [label="{label}"];\n'
            node_dict[node_id] = name
        # Get all edges between these nodes
        self.database.cursor.execute(
            """
            SELECT e.first, e.second
            FROM edges e
            WHERE e.first IN (
                SELECT n.id
                FROM nodes n
                JOIN edges e2 ON n.id = e2.second
                WHERE e2.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            )
            AND e.second IN (
                SELECT n.id
                FROM nodes n
                JOIN edges e2 ON n.id = e2.second
                WHERE e2.first = (SELECT id FROM nodes WHERE name = ? AND type = 'tem_group')
            )
            """,
            (self.hash, self.hash)
        )
        edges = self.database.cursor.fetchall()
        for first, second in edges:
            dot += f'  {first} -> {second};\n'
        dot += "}\n"
        return dot



class InstanceGroup:
    """Wraps around a set of instance nodes and edges."""
    
    def __init__(self, database: Database):
        self.database = database
        self.graph: Optional[nx.DiGraph] = nx.DiGraph() # Stores
        self.hash : Optional[str] = None

    def register(self, template_group_name: str, roots: dict[str, str]):
        
        """
        Given the template group name (a pipeline that I want to calculate) and the leafs (input data nodes) it creates instance nodes. Node names are calculated based on root nodes and intermediate calculation nodes.
        """
        # Get the template group from the database (from edges where first is the template with the id and )

        template = TemplateGroup(self.database)
        template.construct_graph(template_group_name)

        local_graph = deepcopy(template.graph)

        if template.graph is None:
            raise ValueError("Template graph is not constructed. most likely failed to find template id.")

        # Check if all roots are provided (root_name, root_value)
        for node in template.graph.nodes:
            if template.graph.in_degree(node) == 0: # root node
                if node not in roots:
                    raise ValueError(f"Root node {node} is not provided in the roots dictionary.")
                
        # for the root nodes, rename them based on the provided roots
        for root_name, root_value in roots.items():
            if root_name not in template.graph.nodes:
                raise ValueError(f"Root node {root_name} is not in the template graph.")
            if template.graph.in_degree(root_name) != 0:
                raise ValueError(f"Node {root_name} is not a root node.")
            # rename the node in the template graph
            local_graph = nx.relabel_nodes(local_graph, {root_name: root_value})

        mapping = roots
        # Now each graph and point are uniquely identified by the root nodes and the template.
        for node in nx.topological_sort(local_graph):
            predecessors = nx.ancestors(local_graph, node)
            if not predecessors:
                # Skip root nodes
                continue
            predecessors.add(node) # include the node itself
            hash_input = str(sorted(predecessors)).encode()
            node_hash = hashlib.md5(hash_input).hexdigest() # first
            mapping[node] = node_hash
        
        for node, node_hash in mapping.items():
            local_graph = nx.relabel_nodes(local_graph, {node: node_hash})
        

        # Insert into the database all new calculations
        def replacer(match):
            key = match.group(2)
            return mapping.get(key, match.group(0))

        for node in local_graph.nodes:
            node_type = local_graph.nodes[node]['type']
            if node_type == 'tem_dat':
                self.database.cursor.execute(
                    "INSERT OR IGNORE INTO nodes (name, type) VALUES (?, ?)",
                    (node, "ins_dat")
                )
            elif node_type == 'tem_cal':
                command = local_graph.nodes[node]['command']
                # replace (input(data1)) with the mapping value
                pattern = re.compile(r"(input|output)\((\w+)\)")                
                new_command = pattern.sub(replacer, command)

                self.database.cursor.execute(
                    "INSERT OR IGNORE INTO nodes (name, type, extra) VALUES (?, ?, ?)",
                    (node, "ins_cal", '{"command":' + f'"{new_command}"' + '}' )
                )
            else:
                raise ValueError(f"Unknown node type {node_type} in template graph.")

        # TODO: Might want to add edges between calculations.

        for first, second in local_graph.edges:
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES ((SELECT id FROM nodes WHERE name = ?),
                    (SELECT id FROM nodes WHERE name = ?)
                )
                """,
                (first, second)
            )


        # Insert edges between group and all template nodes
        for template_name, instance_name in mapping.items():
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES ((SELECT id FROM nodes WHERE name = ?),
                    (SELECT id FROM nodes WHERE name = ?)
                )
                """,
                (template_name, instance_name)
            )

        # extend the graph by the local graph
        if self.graph is None:
            self.graph = local_graph
        else: 
            self.graph.update(local_graph)



    def commit(self) -> str:
        if self.graph is None:
            raise ValueError("In memory graph is not defined. Either it was never created or already committed.")

        # Create a unique hash for the template group
        group_hash = self._hash()

        self.database.cursor.execute(
            "INSERT OR IGNORE INTO nodes (name, type, extra) VALUES (?, ?, ?)",
            (group_hash, "ins_group", None)
        )
        # Insert edges between group and all template nodes
        for node in self.graph.nodes:
            self.database.cursor.execute(
                """
                INSERT OR IGNORE INTO edges (first, second)
                VALUES (
                    (SELECT id FROM nodes WHERE name = ?),
                    (SELECT id FROM nodes WHERE name = ?)
                )
                """,
                (group_hash, node)
            )

        self.database.conn.commit()
        self.graph = None
        self.hash = group_hash
        return group_hash


    def get_commands(self, template_name: str):
        """Get all the commands for a certain template name"""
        if self.graph is not None:
            raise ValueError("Wrong mode. Please commit before running.")
        
        self.database.cursor.execute(
            """
            SELECT json_extract(n.extra, '$.command') AS command
            FROM nodes n
            JOIN edges e1 ON n.id = e1.second
            JOIN nodes n1 ON e1.first = n1.id
                AND n1.type = 'ins_group'
                AND n1.name = ?
            JOIN edges e2 ON n.id = e2.second
            JOIN nodes n2 ON e2.first = n2.id
                AND n2.type = 'tem_cal'
                AND n2.name = ?
            """,
            (self.hash, template_name)
        )
        nodes = self.database.cursor.fetchall()     

        return list(map(lambda x: x[0], nodes))



    def get_table(self, columns: list[str] | str):
        """
        gets calculation names as a table, where each row corresponds to a unique path through the diagram.
        """

        if self.graph is not None:
            raise ValueError("Wrong mode. Please commit before running.")
        
        if isinstance(columns, str):
            columns = [columns]

        # Get data that belongs to under a certain calculation
        # get the nodes

        # select nodes of inetrest

        nodes = []
        for column in columns:
            self.database.cursor.execute(
                f"""
                SELECT n.id
                FROM nodes n
                JOIN edges e1 ON n.id = e1.second
                JOIN nodes n1 ON e1.first = n1.id
                    AND n1.type = 'ins_group'
                    AND n1.name = ?
                JOIN edges e2 ON n.id = e2.second
                JOIN nodes n2 ON e2.first = n2.id
                    AND n2.type IN ('tem_cal', 'tem_dat')
                    AND n2.name = ?
                """,
                (self.hash, column)
            )
            ns = self.database.cursor.fetchall()  
            nodes.append(list(map(lambda x: x[0], ns)))
        
        print(nodes)

        # select all nodes
        self.database.cursor.execute(
            """
            SELECT n.name
            FROM nodes n
            JOIN edges e1 ON n.id = e1.second
            JOIN nodes n1 ON e1.first = n1.id
                AND n1.type = 'ins_group'
                AND n1.name = ?
            """,
            (self.hash,)
        )
        all_nodes = self.database.cursor.fetchall()    
        # get the edges
        self.database.cursor.execute(
            """
            SELECT src.id AS source_name, tgt.id AS target_name
            FROM edges e
            JOIN nodes src ON e.first = src.id
            JOIN nodes tgt ON e.second = tgt.id
            WHERE src.id IN (
                SELECT n.id
                FROM nodes n
                JOIN edges e1 ON n.id = e1.second
                JOIN nodes n1 ON e1.first = n1.id
                    AND n1.type = 'ins_group'
                    AND n1.name = ?
            )
            OR tgt.id IN (
                SELECT n.id
                FROM nodes n
                JOIN edges e1 ON n.id = e1.second
                JOIN nodes n1 ON e1.first = n1.id
                    AND n1.type = 'ins_group'
                    AND n1.name = ?
            )
            """,
            (self.hash, self.hash)
        )

        edges = self.database.cursor.fetchall() 

        # construct a graph  
        G = nx.DiGraph()
        G.add_nodes_from(all_nodes)
        G.add_edges_from(edges)

        def path_exists_through_nodes(G, nodes):
            for i in range(len(nodes) - 1):
                if not nx.has_path(G, nodes[i], nodes[i+1]):
                    return False
            return True
        
        from itertools import product

        for combo in product(*nodes):
            is_path = path_exists_through_nodes(G, combo)
            print(combo, is_path)


    def register_instance_group(self, instance_group: InstanceGroup):
        pass

    def delete_and_commit(self):
        pass


    def filter_template(self, template_names: list[str]):
        pass


    
    def _hash(self):
        """Create a unique hash for the template group."""
        import hashlib
        # Create a sorted representation of the graph
        nodes = sorted(self.graph.nodes)
        edges = sorted((u, v) for u, v in self.graph.edges)
        representation = str(nodes) + str(edges)
        return hashlib.md5(representation.encode()).hexdigest()

class Database:

    def __init__(self, database_path: str | pathlib.Path):
        """Connect to the database. This objcet provides a pathway to all data."""
        self.conn = sqlite3.connect(database_path)
        _init_db(self.conn)
        self.cursor = self.conn.cursor()

    def new_template_group(self) -> TemplateGroup:
        return TemplateGroup(self)

    def new_instance_group(self) -> InstanceGroup:
        return InstanceGroup(self)

    def get_instance_group(self, instance_group_id: list[str] | str) -> InstanceGroup:
        pass

    def as_dot(self):
        """return the whole database as in dot format"""
    
        """Reconstruct the graph from the database."""

        # Query the database to reconstruct the graph
        dot = "digraph G {\n"
        # Get all nodes in the group
        self.cursor.execute(
            """
            SELECT n.id, n.name, n.type, n.extra
            FROM nodes n
            """
        )
        nodes = self.cursor.fetchall()
        node_dict = {}
        for node_id, name, ntype, extra in nodes:
            label = f"{name}"

            dot += f'  {node_id} [label="{label}"];\n'
            node_dict[node_id] = name
        # Get all edges between these nodes
        self.cursor.execute(
            """
            SELECT e.first, e.second
            FROM edges e
            """
        )
        edges = self.cursor.fetchall()
        for first, second in edges:
            dot += f'  {first} -> {second};\n'
        dot += "}\n"
        return dot



if __name__ == "__main__":
    # connect to the database
    db = Database("persistance_tracker.db")


    tg = db.new_template_group()
    tg.register(f"calc1",f"python3 script.py input(common_input) output(data2)")
    tg.register(f"calc2",f"python3 script.py input(data2) input(data1) output(data3)")

    tg_name = tg.commit()

    # now create some calculations
    cg = db.new_instance_group()
    common_input = "hello"
    for i in range(4):
        cg.register(template_group_name = tg_name, roots = {"data1": f"mycustomcooldata{i}",
                                                            "common_input": common_input})

    cg_name = cg.commit()
    # print("commands for calculation1")
    # command_list = cg.get_commands("calc1")
    # print(command_list)
    # print("commands for calculation2")
    # command_list = cg.get_commands("calc2")
    # print(command_list)

    print(cg.get_table( ["data2", "data1"]))

    

    # break
    # tg = db.new_template_group()
    # tg.register("calc1","python3 script.py input(myinput1) output(myoutput1)")
    # template_id = tg.commit() # commit this template. So that multiple could be used as one.

    # cg = db.new_instance_group()
    # cg.register(template_id = template_id, {"myinput1": "hi", "myoutput1": "bye"})
    # cg.register(template_id = template_id, {"myinput1": "hi", "myoutput1": "bye"})
    # instance_id = cg.commit()


    # # Another example, now I want to run a Bayesion optimization loop
    # ## Create a template

    # db = Database() # Connect to the datbasae
    # # Create a group of templates - this wil be a template for a workflow
    # tg = db.new_template_group()
    # tg.register("calc1","python3 script.py input(myinput1) output(myoutput1)")
    # tg.register("calc2","python3 script2.py input(myinput2) output(myoutput2)")
    # tg.register("optimizer","python3 optimizer.py input(optinput) output(optoutput)")
    # template_id = tg.commit() # commit this template. So that multiple could be used as one.

    # # Create a cluster of instances
    # cg1 = db.new_instance_group()
    # for i in range(4):
    #     # I would like to get a instance so that I could run only that one if I want to.
    #     cg2 = db.new_instance_group()
    #     cg2.register(template_id = template_id, {"optinput": "start"})
    #     cg_name = cg2.commit()
        
    #     script = cg2.to_bash() # convert to a basch script (could have other formats)
    #     run_slurm(script)

    #     # with each iteraciton cg1 changes, so I need to delete the old one.
    #     cg1.register_instance_group(cg2)
    #     cg1.delete_and_commit() 

    # # In this case just overwrite what I have done iteratively.
    # cg_name = cg1.commit()


    # # In some other program
    # db = Database() # Connect to the datbasae
    # cg = db.get_instance_group(cg_name)
    # cg.filter_template(["a", "b"]) # select only these templates
    # print(cg)
    
