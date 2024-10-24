<a href="https://datahub.io/core/london-life-expectancy"><img src="https://badgen.net/badge/icon/View%20on%20datahub.io/orange?icon=https://datahub.io/datahub-cube-badge-icon.svg&label&scale=1.25)" alt="badge" /></a>

This dataset was scraped from [London data](https://data.london.gov.uk/) website.
Life expectancy at birth and age 65 by sex.
Data for 2000-2002 to 2008-2010 revised on 24 July 2013.
Local authorities based on boundaries as of 2010.
England and Wales figures - non-resident deaths included.
Figures given for 3 combined years to increase reliability at local levels.

## Data
Dataset used for this scraping have been found on [Life Expectancy at Birth and at Age 65, Borough](https://data.london.gov.uk/dataset/life-expectancy-birth-and-age-65-borough).
 
Output data is located in `data` directory, it consists of three `csv` files:
* `male-life-expectancy.csv`
* `female-life-expectancy.csv`
* `life-expectancy-at-65.csv`

## Preparation
You will need Python 3.6 or greater and dataflows library to run the script

To update the data run the process script locally:

```
# Install dataflows
pip install dataflows

# Run the script
python london-life-expectancy.py
```

### License

Open Government Licence

> You are encouraged to use and re-use the Information that is available under this licence freely and flexibly, with only a few conditions.
Using Information under this licence
>Use of copyright and database right material expressly made available under this licence (the 'Information') indicates your acceptance of the terms and conditions below.
> The Licensor grants you a worldwide, royalty-free, perpetual, non-exclusive licence to use the Information subject to the conditions below.
> This licence does not affect your freedom under fair dealing or fair use or any other copyright or database right exceptions and limitations.

You may find further information [here](http://www.nationalarchives.gov.uk/doc/open-government-licence/version/3/)

