To create and add a new calculation

```bash
graphr new-calculation first_double "python3 scripts.py double --input input(file1.yaml) --output output(file2.yaml)" | graphr add
```

To make a graph:

```bash
graphr get | graphr show | dot -Tpdf > data/graph.pdf
```