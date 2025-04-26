from graphviz import Digraph
dot = Digraph()
dot.node('A', 'Test Node')
dot.node('B', 'Test Node B')
dot.edge('A', 'B')
dot.render('test_graph', format='png', view=False)