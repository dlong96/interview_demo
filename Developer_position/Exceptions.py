
class NotExistedError(Exception):
    """
    raise if the given argument string is a typo
    for instance, when adding a index but the given index name doesn't exist in the collection 
    """
    def __init__(self):
        Exception.__init__(self,"Not Exists") 
