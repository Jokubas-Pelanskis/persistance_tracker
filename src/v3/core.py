from __future__ import annotations
import sqlite3
import pathlib
from copy import deepcopy
import re
import hashlib
from typing import Any, Dict, List, Optional, Tuple
import re
import networkx as nx
import kuzu
import json
DB_PATH = "persistance_tracker.db"


class TemplateGroup:
    """Wraps around a set of templates nodes and edges."""

    def __init__(self, database: Database):
        self.database = database
        self.hash : Optional[str] = None
        self.data_nodes: list[str] | None = None
        self.calculation_nodes: list[str] | None = None

    def register(self, name: str, command: str):
        """
        Given the name and the command of the template it creates template nodes
        What it does:
        - Extract inputs and outputs using regex
        - Create calculation nodes and all relevant data nodes. and insert into the database.
        """
        if self.data_nodes is None:
            self.data_nodes = []
        if self.calculation_nodes is None:
            self.calculation_nodes = []

        # extract inputs, outputs and the command
        inputs = re.findall(r'input\((.*?)\)', command)
        outputs = re.findall(r'output\((.*?)\)', command)

        self.database.conn.execute("MERGE (:Template_Calculation {name: $name, command: $command})", parameters = {"name": name, "command": command})

        for inp in inputs:
            self.database.conn.execute("MERGE (:Template_Data {name: $name})", parameters = {"name": inp})
            self.database.conn.execute("""
                MATCH (td:Template_Data {name:$input_name}), (tc:Template_Calculation {name:$calculation_name})
                MERGE (td)-[r:TEMPLATE_INPUT]->(tc)
            """,
            parameters = {"input_name": inp, "calculation_name" : name})


        for out in outputs:

            self.database.conn.execute("MERGE (:Template_Data {name: $name})", parameters = {"name": out})
            self.database.conn.execute("""
                MATCH (td:Template_Data {name:$output_name}), (tc:Template_Calculation {name:$calculation_name})
                MERGE (tc)-[r:TEMPLATE_OUTPUT]->(td)
            """,
            parameters = {"output_name": out, "calculation_name" : name})

        self.calculation_nodes.append(name)
        self.data_nodes.extend(inputs)
        self.data_nodes.extend(outputs)


    def commit(self) -> str:
        """
        Commit the template group to the database.
        Add tem_group node and 
        """
        assert self.data_nodes is not None
        assert self.calculation_nodes is not None

        template_group_name = self._hash()
        self.database.conn.execute("MERGE (:Template_Group {name: $name})", parameters = {"name": template_group_name})

        for node in self.data_nodes:
            self.database.conn.execute("""
                MATCH (tg:Template_Group {name:$template_group_name}), (td:Template_Data {name:$data_name})
                MERGE (tg)-[r:TEMPLATE_DATA_GROUPS]->(td)
            """,
            parameters = {"template_group_name": template_group_name, "data_name" : node})

        for node in self.calculation_nodes:
            self.database.conn.execute("""
                MATCH (tg:Template_Group {name:$template_group_name}), (tc:Template_Calculation {name:$calculation_name})
                MERGE (tg)-[r:TEMPLATE_CALCULATION_GROUPS]->(tc)
            """,
            parameters = {"template_group_name": template_group_name, "calculation_name" : node})
        
        return template_group_name

    def _hash(self) -> str:
        """Create a unique hash for the template group."""
        if self.data_nodes is None:
            raise ValueError("no nodes are store. Cannot calculate hash.")
        if self.calculation_nodes is None:
            raise ValueError("no nodes are store. Cannot calculate hash.")
        
        combined = "|".join(self.data_nodes + self.calculation_nodes)


        # Compute MD5 hash
        hash_value = hashlib.md5(combined.encode("utf-8")).hexdigest()
        return hash_value
    

    def as_dot(self) -> str:
        pass


class InstanceGroup:
    """Wraps around a set of instance nodes and edges."""
    
    def __init__(self, database: Database):
        self.database = database
        self.hash : Optional[str] = None
        self.data_nodes: list[str] | None = None
        self.calculation_nodes: list[str] | None = None

    def register(self, template_group_name: str, roots: dict[str, str]):
        # check if roots are correct before moving on.

        if self.data_nodes is None:
            self.data_nodes = []

        if self.calculation_nodes is None:
            self.calculation_nodes = []

        # create a string to calculate a hash
        roots_string = json.dumps(roots, sort_keys=True)

        # ------------
        # Find all calculation in the template
        calculation_nodes = self.database.conn.execute("""
                MATCH (tg: Template_Group {name:$template_group})-[r:TEMPLATE_CALCULATION_GROUPS]->(target)
                RETURN target.name, target.command
            """,
            parameters = {"template_group": template_group_name})
        


        # Go through all the calculations
        data_mapping = {} # {template_name: instance_name}
        calculation_mapping = {}
        for c_node_d in list(calculation_nodes):
            c_node = c_node_d[0]
            c_node_command = c_node_d[1]
            print(c_node_command)

            # Get inputs and output template names

            input_template_name = self.database.conn.execute("""
                MATCH (inputs: Template_Data)-[r:TEMPLATE_INPUT]->(tg: Template_Calculation {name: $calculation_name})
                RETURN inputs.name
            """,
            parameters = {"calculation_name": c_node})
            output_template_name = self.database.conn.execute("""
                MATCH (calculation: Template_Calculation {name : $calculation_name})-[r:TEMPLATE_OUTPUT]->(output: Template_Data)
                RETURN output.name
            """,
            parameters = {"calculation_name": c_node})

            # Create name for the calculation node
            history_nodes = self.database.conn.execute("""
                    MATCH (ancestor)-[r:TEMPLATE_INPUT|TEMPLATE_OUTPUT*]->(target:Template_Calculation {name: $name})
                    RETURN ancestor.name
                """,
                parameters = {"name": c_node})
            
            graph_string = "|".join(list(map(lambda x:x[0], history_nodes)))
            hash_string = graph_string + c_node + roots_string # combine all information to uniquely 
            hash_name_calculation = hashlib.md5((hash_string).encode('utf-8')).hexdigest()
            calculation_mapping[c_node] = hash_name_calculation

            ## insert link from template to calculation
            self.database.conn.execute("MERGE (:Instance_Calculation {name: $name})", parameters = {"name": hash_name_calculation})
            self.database.conn.execute("""
                        MATCH (td:Template_Calculation {name:$template_name}), (tc:Instance_Data {name:$calculation_name})
                        MERGE (td)-[r:TEMPLATE_TO_INSTANCE_CALCULATION]->(tc)
                    """,
                    parameters = {"template_name": c_node, "calculation_name" : hash_name_calculation})     


            # create names for the data nodes
            for data_node in (list(input_template_name) + list(output_template_name)):
                if data_node[0] not in data_mapping:
                    history_nodes = self.database.conn.execute("""
                            MATCH (ancestor)-[r:TEMPLATE_INPUT|TEMPLATE_OUTPUT*]->(target:Template_Data {name: $name})
                            RETURN ancestor.name
                        """,
                        parameters = {"name": data_node[0]})
                    graph_string = "|".join(list(map(lambda x:x[0], history_nodes)))
                    hash_string = graph_string + data_node[0] + roots_string # combine all information to uniquely identify the node
                    hash_name = hashlib.md5((hash_string).encode('utf-8')).hexdigest()

                    data_mapping[data_node[0]] = hash_name

                    # Insert a link from template to the instance
                    self.database.conn.execute("MERGE (:Instance_Data {name: $name})", parameters = {"name": hash_name})
                    self.database.conn.execute("""
                        MATCH (td:Template_Data {name:$template_name}), (tc:Instance_Data {name:$data_name})
                        MERGE (td)-[r:TEMPLATE_TO_INSTANCE_DATA]->(tc)
                    """,
                    parameters = {"template_name": data_node[0], "data_name" : hash_name})             

        return



        # ------------

        # Find all all template nodes under a certain group
        result = self.database.conn.execute("""
                MATCH (tg: Template_Group {name:$template_group})-[r:TEMPLATE_DATA_GROUPS]->(target)
                RETURN target.name
            """,
            parameters = {"template_group": template_group_name})

        for node in list(map(lambda x: x[0], result)):
            # search for all the history nodes
            r1 = self.database.conn.execute("""
                    MATCH (ancestor)-[r:TEMPLATE_INPUT|TEMPLATE_OUTPUT*]->(target:Template_Data {name: $name})
                    RETURN ancestor.name
                """,
                parameters = {"name": node})
            
            graph_string = "|".join(list(map(lambda x:x[0], r1)))
            hash_name = hashlib.md5((graph_string + roots_string).encode('utf-8')).hexdigest()
            
            # create new nodes

            self.data_nodes.append(hash_name)
            self.database.conn.execute("MERGE (:Instance_Data {name: $name})", parameters = {"name": hash_name})
            self.database.conn.execute("""
                MATCH (td:Template_Data {name:$template_name}), (tc:Instance_Data {name:$data_name})
                MERGE (td)-[r:TEMPLATE_TO_INSTANCE_DATA]->(tc)
            """,
            parameters = {"template_name": node, "data_name" : hash_name})            



    def commit(self) -> str:
        pass


    def get_commands(self, template_name: str):
        """Get all the commands for a certain template name"""




    def get_table(self, columns: list[str] | str):
        """
        gets calculation names as a table, where each row corresponds to a unique path through the diagram.
        """


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
        db = kuzu.Database(database_path)
        self.conn = kuzu.Connection(db)
        self.conn.execute("CREATE NODE TABLE Template_Data(name STRING, PRIMARY KEY(name))")
        self.conn.execute("CREATE NODE TABLE Template_Calculation(name STRING, command STRING, PRIMARY KEY(name))")
        self.conn.execute("CREATE NODE TABLE Template_Group(name STRING, PRIMARY KEY(name))")
        self.conn.execute("CREATE NODE TABLE Instance_Data(name STRING, PRIMARY KEY(name))")
        self.conn.execute("CREATE NODE TABLE Instance_Calculation(name STRING, command STRING, PRIMARY KEY(name))")
        self.conn.execute("CREATE NODE TABLE Instance_Group(name STRING, PRIMARY KEY(name))")


        self.conn.execute("CREATE REL TABLE TEMPLATE_INPUT(FROM Template_Data TO Template_Calculation)")
        self.conn.execute("CREATE REL TABLE TEMPLATE_OUTPUT(FROM Template_Calculation TO Template_Data)")

        self.conn.execute("CREATE REL TABLE INSTANCE_INPUT(FROM Instance_Data TO Instance_Calculation)")
        self.conn.execute("CREATE REL TABLE INSTANCE_OUTPUT(FROM Instance_Calculation TO Instance_Data)")

        self.conn.execute("CREATE REL TABLE TEMPLATE_DATA_GROUPS(FROM Template_Group TO Template_Data)")
        self.conn.execute("CREATE REL TABLE TEMPLATE_CALCULATION_GROUPS(FROM Template_Group TO Template_Calculation)")

        self.conn.execute("CREATE REL TABLE TEMPLATE_TO_INSTANCE_DATA(FROM Template_Data TO Instance_Data)")
        self.conn.execute("CREATE REL TABLE TEMPLATE_TO_INSTANCE_CALCULATION(FROM Template_Data TO Instance_Calculation)")
        

    def new_template_group(self) -> TemplateGroup:
        return TemplateGroup(self)

    def new_instance_group(self) -> InstanceGroup:
        return InstanceGroup(self)

    def get_instance_group(self, instance_group_id: list[str] | str) -> InstanceGroup:
        pass

    def as_dot(self):
        """return the whole database as in dot format"""
    
        dot_lines = ["digraph G {"]
        # Query nodes
        nodes = self.conn.execute("MATCH (n) RETURN DISTINCT n").get_as_df()

        # Query relationships
        rels = self.conn.execute("MATCH (a)-[r]->(b) RETURN a, r, b").get_as_df()
        # Add nodes
        for _, row in nodes.iterrows():
            node = row["n"]
            # Use primary key as identifier
            label = node["name"]
            dot_lines.append(f'  "{label}" [label="{label}"];')

        # Add edges
        for _, row in rels.iterrows():
            src = row["a"]["name"]
            dst = row["b"]["name"]


            dot_lines.append(f'  "{src}" -> "{dst}";')

        dot_lines.append("}")

        dot_output = "\n".join(dot_lines)
        return dot_output



if __name__ == "__main__":
    # connect to the database
    db = Database("persistance_tracker.db")

    
    tg = db.new_template_group()
    tg.register(f"calc1",f"python3 script.py input(common_input) output(data2)")
    tg.register(f"calc2",f"python3 script.py input(data2) input(data1) output(data3)")

    # for i in range(1000):
    #     tg.register(f"calc{i}",f"python3 script.py input(data{i}) output(data{i+1})")
    tg_name = tg.commit()

    # now create some calculations
    cg = db.new_instance_group()
    common_input = "hello"
    for i in range(4):
        cg.register(template_group_name = tg_name, roots = {"data1": f"mycustomcooldata{i}",
                                                            "common_input": common_input})
    print(db.as_dot())
    exit
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
    
