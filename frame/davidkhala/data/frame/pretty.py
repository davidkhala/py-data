
def md_from(df):
    """
    markdown style
    """
    from tabulate import tabulate
    return tabulate(df, headers='keys', tablefmt='github')