Examples:


```python
# Create a minimal example of the behaviour for future reference

# Create a global db that stores everything
global_db = gt.Database()

# Create some other small worfklow
## Define the template
db1 = gt.Database()

f1 = db1.template_register_dnode("f1")
f2 = db1.template_register_dnode("f2")
f3 = db1.template_register_dnode("f3")
f4 = db1.template_register_dnode("f4")

p1 = db1.template_register_cnode("p1",f"this input({f1}) output({f2})")
p2 = db1.template_register_cnode("p2",f"that input({f2}) input({f3}) output({f4})")



# -------------------------------------------------------
## Add some calculations
c =  db1.template_create_calculation(leafs = {"f1": "my_custom_filename", "f3":"f3_analysis1"})
c.merge_into(db1)
c =  db1.template_create_calculation(leafs = {"f1": "my_custom_filename", "f3":"f3_analysis2"})
c.merge_into(db1)

print("db1")
display(Source(db1.template_as_dot()))
display(Source(db1.as_dot()))

# -----------------------------------------------------------
# Create another with the same template, just different data

db3 = db1.generate_empty()
db3 =  db3.template_create_calculation(leafs = {"f1": "my_custom_filename", "f3":"f3_analysis3"})

print("db3")
display(Source(db3.template_as_dot()))
display(Source(db3.as_dot()))

# -----------------------------------------------------------
# Now create another, but similar template with modified workflow
db2 = gt.Database()

f1 = db2.template_register_dnode("f1")
f2 = db2.template_register_dnode("f2")
f3 = db2.template_register_dnode("f3")
f4 = db2.template_register_dnode("f4")

p1 = db2.template_register_cnode("p1",f"this input({f1}) output({f2})")
p2 = db2.template_register_cnode("p2xx",f"that input({f2}) input({f3}) output({f4})")



# -----------------------------------------------------------
## Add some calculations
c =  db2.template_create_calculation(leafs = {"f1": "my_custom_filename", "f3":"f3_analysis1"})
c.merge_into(db2)
c =  db2.template_create_calculation(leafs = {"f1": "my_custom_filename", "f3":"f3_analysis2"})
c.merge_into(db2)


print("db2")
display(Source(db2.template_as_dot()))
display(Source(db2.as_dot()))

# -----------------------------------------------------------
# now merge into the global database
db1.merge_into(global_db)

## Merge something into the database, that has a different profile
db2.merge_into(global_db)
## Merge again - this should not change anything
db1.merge_into(global_db)

## Now merge the third database - should just add lacking calculations
db3.merge_into(global_db)
print("global_db")
display(Source(global_db.template_as_dot()))
display(Source(global_db.as_dot()))


# -----------------------------------------------------------
# Now find all calculations from the global db that foll under p2

for p2 in global_db.select_similar("f3"):
    print(f"Under input f3 there are the following calculations:", p2.id)
    print("p2:")
    for c in global_db.select_future(p2.id).select_similar("p2"):
        print(c.id)
    print("p2xx:")
    for c in global_db.select_future(p2.id).select_similar("p2xx"):
        print(c.id)
        
    
```