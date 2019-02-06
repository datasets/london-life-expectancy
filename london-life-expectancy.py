from dataflows import Flow, load, dump_to_path, ResourceWrapper, PackageWrapper, unpivot


def set_format_and_name(package: PackageWrapper):

    package.pkg.descriptor['title'] = 'London Life expectancy'
    package.pkg.descriptor['name'] = 'london-life-expectancy'

    package.pkg.descriptor['licenses'] = [{
        "name": "OGL",
        "path": 'http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/',
        "title": 'UK Open Government Licence'
    }]

    package.pkg.descriptor['resources'][0]['path'] = 'data/male-life-expectancy.csv'
    package.pkg.descriptor['resources'][0]['name'] = 'male-life-expectancy'

    package.pkg.descriptor['resources'][1]['path'] = 'data/female-life-expectancy.csv'
    package.pkg.descriptor['resources'][1]['name'] = 'female-life-expectancy'

    package.pkg.descriptor['resources'][2]['path'] = 'data/life-expectancy-at-65.csv'
    package.pkg.descriptor['resources'][2]['name'] = 'life-expectancy-at-65'

    yield package.pkg
    res_iter = iter(package)
    first: ResourceWrapper = next(res_iter)
    second: ResourceWrapper = next(res_iter)
    third: ResourceWrapper = next(res_iter)

    yield first.it
    yield second.it
    yield third.it

    yield from package


def remove_duplicates(rows):
    seen = set()
    for row in rows:
        line = ''.join('{}{}'.format(key, val) for key, val in row.items())
        if line in seen: continue
        seen.add(line)
        yield row


link = 'https://data.london.gov.uk/download/life-expectancy-birth-and-age-65-borough/' \
       'b37eea33-4f3b-4c54-8590-4b68ba241165/life-expectancy-birth-over65-borough.xls'

unpivot_fields = [
    {'name': '1991-1993', 'keys': {'Year': '1991-1993'}},
    {'name': '1992-1994', 'keys': {'Year': '1992-1994'}},
    {'name': '1993-1995', 'keys': {'Year': '1993-1995'}},
    {'name': '1994-1996', 'keys': {'Year': '1994-1996'}},
    {'name': '1995-1997', 'keys': {'Year': '1995-1997'}},
    {'name': '1996-1998', 'keys': {'Year': '1996-1998'}},
    {'name': '1997-1999', 'keys': {'Year': '1997-1999'}},
    {'name': '1998-2000', 'keys': {'Year': '1998-2000'}},
    {'name': '1999-2001', 'keys': {'Year': '1999-2001'}},
    {'name': '2000-2002', 'keys': {'Year': '2000-2002'}},
    {'name': '2001-2003', 'keys': {'Year': '2001-2003'}},
    {'name': '2002-2004', 'keys': {'Year': '2002-2004'}},
    {'name': '2003-2005', 'keys': {'Year': '2003-2005'}},
    {'name': '2004-2006', 'keys': {'Year': '2004-2006'}},
    {'name': '2005-2007', 'keys': {'Year': '2005-2007'}},
    {'name': '2006-2008', 'keys': {'Year': '2006-2008'}},
    {'name': '2007-2009', 'keys': {'Year': '2007-2009'}},
    {'name': '2008-2010', 'keys': {'Year': '2008-2010'}},
    {'name': '2009-2011', 'keys': {'Year': '2009-2011'}},
    {'name': '2010-2012', 'keys': {'Year': '2010-2012'}},
    {'name': '2011-2013', 'keys': {'Year': '2011-2013'}},
    {'name': '2012-2014', 'keys': {'Year': '2012-2014'}}
]
extra_keys = [
    {'name': 'Year', 'type': 'any'}
]
extra_value = {'name': 'Value', 'type': 'any'}

Flow(
    load(link,
         format="xls",
         skip_rows=[''],
         fill_merged_cells=True,
         sheet=2),
    load(link,
         format="xls",
         skip_rows=[''],
         fill_merged_cells=True,
         sheet=3),
    unpivot(unpivot_fields, extra_keys, extra_value),
    load(link,
         format="xls",
         headers=[1, 2],
         skip_rows=[''],
         fill_merged_cells=True,
         sheet=4),
    set_format_and_name,
    remove_duplicates,
    dump_to_path(),
).process()
