# Song Play Analysis ETL Pipeline

In order to understand user engagement, Sparkify is pulling song data from its music streaming app.
Better insights in what our users are listening to will help our analytics team create a better app and
a better experience for our listeners.

### Relational Model for Better Analytics

To get accurate analytics, we need robust data. For this reason, we are using a relational data model. This script takes 
raw `json` files, cleans them, and puts them into a postgres database. From here, our analytics team can access user data
with the confidence in its accuracy.

