from dataflows import Flow, load, dump_to_path, ResourceWrapper, PackageWrapper, unpivot


def set_format_and_name(package: PackageWrapper):

    package.pkg.descriptor['title'] = 'London life expectancy'
    package.pkg.descriptor['name'] = 'life-expectancy'

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
    {'name': '([0-9]{4})-([0-9]{4})', 'keys': {'Year': r'\2''-01-01'}}
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
