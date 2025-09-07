import graphrlib as gl


"""
how do I want to use the API. 

I need:
1) Create the template
2) Create all the calculations
3) 
"""

# I want  to be able to add calculations and then filter calculations too.
# Saving everything to the database

# every
db = gl.Database(id = calculation_id) # do not do that becasue that adds restriction. (i cannot construct multiple calculations at the same time)
db = gl.Database() # do this and then add calculation-id 
# When do you generate the id? What is id? What does it identify: a specific calculation? An instance of a calculation

# generate a new id for every run (new id, when I come back)
for root_name in root_names:
    db.add_calculation(root_name)

# now I want to modify the caluclations (say add extra or change the parameter if somtehing has crashed)
for new_root_name in root_names_new:
    db.add_calculation(new_root_name)



# get an already constructed calculation (without being able to change it); but add a copy functionality to to create a database that you can modify. That allso creates a new id.
db = gl.get_database([calculation_id]) # if None is provided, then it connects to the whole databsae. If a list is provided, then multiple datbases are connected together.

# copy the database, this creates a database
new_db = db.copy()

# Generate an empty calculation to be built
db, calculation_id = gl.create_empty()

db.template_register_dnode()
db.template_register_cnode()

db.template_select_history("template_name") # find all nodes that do not fit the filter and then remove them from the template 


db.add_calculation(...) # for the template calculete all the calculation node names.

# querying and finding information in the database
names = db.get(['name1', 'name2']) # returns a table.

# Now I can do whatever I want with that. (Could write helper functions to convert to different formats.) and transform the data

new_db = gl.create_from_subset(names) # takes filtered dataset and creates a database





