import csv, os

__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))

cities = []
with open(os.path.join(__location__, 'Cities.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        cities.append(dict(r))

countries = []
with open(os.path.join(__location__, 'Countries.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        countries.append(dict(r))

players = []
with open(os.path.join(__location__, 'Players.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        players.append(dict(r))

teams = []
with open(os.path.join(__location__, 'Teams.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        teams.append(dict(r))

titanic = []
with open(os.path.join(__location__, 'Titanic.csv')) as f:
    rows = csv.DictReader(f)
    for r in rows:
        titanic.append(dict(r))
class DB:
    def __init__(self):
        self.database = []

    def insert(self, table):
        self.database.append(table)

    def search(self, table_name):
        for table in self.database:
            if table.table_name == table_name:
                return table
        return None
    
import copy
class Table:
    def __init__(self, table_name, table):
        self.table_name = table_name
        self.table = table
    
    def join(self, other_table, common_key):
        joined_table = Table(self.table_name + '_joins_' + other_table.table_name, [])
        for item1 in self.table:
            for item2 in other_table.table:
                if item1[common_key] == item2[common_key]:
                    dict1 = copy.deepcopy(item1)
                    dict2 = copy.deepcopy(item2)
                    dict1.update(dict2)
                    joined_table.table.append(dict1)
        return joined_table
    
    def filter(self, condition):
        filtered_table = Table(self.table_name + '_filtered', [])
        for item1 in self.table:
            if condition(item1):
                filtered_table.table.append(item1)
        return filtered_table

    def __is_float(self, element):
        if element is None:
            return False
        try:
            float(element)
            return True
        except ValueError:
            return False

    def aggregate(self, function, aggregation_key):
        temps = []
        for item1 in self.table:
            if self.__is_float(item1[aggregation_key]):
                temps.append(float(item1[aggregation_key]))
            else:
                temps.append(item1[aggregation_key])
        return function(temps)
    
    def select(self, attributes_list):
        temps = []
        for item1 in self.table:
            dict_temp = {}
            for key in item1:
                if key in attributes_list:
                    dict_temp[key] = item1[key]
            temps.append(dict_temp)
        return temps

    def __str__(self):
        return self.table_name + ':' + str(self.table)

    # code for other methods

    def pivot_table(self, keys_to_pivot_list, keys_to_aggregate_list, aggregate_func_list):

        unique_values_list = []
        for key in keys_to_pivot_list:
            mini = []
            for data in self.table:
                if data[key] not in mini:
                    mini.append(data[key])
            unique_values_list.append(mini)

        import combination_gen
        gen_comb_list = combination_gen.gen_comb_list(unique_values_list)

        pivot_table = []
        for comb_list in gen_comb_list:
            filtered = copy.deepcopy(self)
            for comb in comb_list:
                filtered = filtered.filter(lambda x: x[keys_to_pivot_list[comb_list.index(comb)]] == comb)
            wanted_val_list = []
            for agg_func in aggregate_func_list:
                wanted_val = filtered.aggregate(agg_func, keys_to_aggregate_list[aggregate_func_list.index(agg_func)])
                wanted_val_list.append(wanted_val)

            pivot_table.append([comb_list, wanted_val_list])

        return pivot_table

table1 = Table('cities', cities)
table2 = Table('countries', countries)
my_DB = DB()
my_DB.insert(table1)
my_DB.insert(table2)
my_table1 = my_DB.search('cities')

print("Test filter: only filtering out cities in Italy")
my_table1_filtered = my_table1.filter(lambda x: x['country'] == 'Italy')
print(my_table1_filtered)
print()

print("Test select: only displaying two fields, city and latitude, for cities in Italy")
my_table1_selected = my_table1_filtered.select(['city', 'latitude'])
print(my_table1_selected)
print()

print("Calculting the average temperature without using aggregate for cities in Italy")
temps = []
for item in my_table1_filtered.table:
    temps.append(float(item['temperature']))
print(sum(temps)/len(temps))
print()

print("Calculting the average temperature using aggregate for cities in Italy")
print(my_table1_filtered.aggregate(lambda x: sum(x)/len(x), 'temperature'))
print()

print("Test join: finding cities in non-EU countries whose temperatures are below 5.0")
my_table2 = my_DB.search('countries')
my_table3 = my_table1.join(my_table2, 'country')
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'no').filter(lambda x: float(x['temperature']) < 5.0)
print(my_table3_filtered.table)
print()
print("Selecting just three fields, city, country, and temperature")
print(my_table3_filtered.select(['city', 'country', 'temperature']))
print()

print("Print the min and max temperatures for cities in EU that do not have coastlines")
my_table3_filtered = my_table3.filter(lambda x: x['EU'] == 'yes').filter(lambda x: x['coastline'] == 'no')
print("Min temp:", my_table3_filtered.aggregate(lambda x: min(x), 'temperature'))
print("Max temp:", my_table3_filtered.aggregate(lambda x: max(x), 'temperature'))
print()

print("Print the min and max latitude for cities in every country")
for item in my_table2.table:
    my_table1_filtered = my_table1.filter(lambda x: x['country'] == item['country'])
    if len(my_table1_filtered.table) >= 1:
        print(item['country'], my_table1_filtered.aggregate(lambda x: min(x), 'latitude'), my_table1_filtered.aggregate(lambda x: max(x), 'latitude'))
print()
print('---Week12---')
print()
print('---Task1---')
table4 = Table('players', players)
# print(table4.table)
table5 = Table('teams', teams)
# print(table5.table)
table6 = Table('titanic', titanic)
table7 = table4.join(table5, 'team')
# print(table7.table)
print('Player on a team with “ia” in the team name played less than 200 minutes and made more than 100 passes:')
table7_filtered = table7.filter(lambda x: int(x['minutes']) < 200).filter(lambda x: int(x['passes']) > 100).filter(lambda x: 'ia' in x['team'])
for player in table7_filtered.table:
    print(player['surname'], player['team'], player['position'])
print()

print('The average number of games played for teams ranking below 10 versus teams ranking above or equal 10:')
table5_filtered_top9 = table5.filter(lambda x: int(x['ranking']) < 10)
table5_filtered_10below = table5.filter(lambda x: int(x['ranking']) >= 10)
print('ranking below 10:', table5_filtered_top9.aggregate(lambda x: float(sum(x)/len(table5_filtered_top9.table)), 'games'))
print('ranking above or equal 10:', table5_filtered_10below.aggregate(lambda x: float(sum(x)/len(table5_filtered_10below.table)), 'games'))
print()

print('The average number of passes made by forwards versus by midfielders:')
table4_filtered_mf = table4.filter(lambda x: x['position'] == 'midfielder')
table4_filtered_fw = table4.filter(lambda x: x['position'] == 'forward')
print('midfielder:', table4_filtered_mf.aggregate(lambda x: float(sum(x)/len(table4_filtered_mf.table)), 'passes'))
print('forward:', table4_filtered_fw.aggregate(lambda x: float(sum(x)/len(table4_filtered_fw.table)), 'passes'))
print()

print('The average fare paid by passengers in the first class versus in the third class')
table6_filtered_1c = table6.filter(lambda x: x['class'] == '1')
table6_filtered_3c = table6.filter(lambda x: x['class'] == '3')
print('First class:', table6_filtered_1c.aggregate(lambda x: float(sum(x)/len(table6_filtered_1c.table)),'fare'))
print('Third class:', table6_filtered_3c.aggregate(lambda x: float(sum(x)/len(table6_filtered_3c.table)),'fare'))
print()

print('The survival rate of male versus female passengers:')
table6_filtered_male = table6.filter(lambda x: x['gender'] == 'M')
table6_filtered_female = table6.filter(lambda x: x['gender'] == 'F')
table6_filtered_male_survived = table6_filtered_male.filter(lambda x: x['survived'] == 'yes')
table6_filtered_female_survived = table6_filtered_female.filter(lambda x: x['survived'] == 'yes')
print('Male:', len(table6_filtered_male_survived.table)/len(table6_filtered_male.table)*100, '%')
print('Female:', len(table6_filtered_female_survived.table)/len(table6_filtered_female.table)*100, '%')
print()
print('---Task3---')

print('The total number of male passengers embarked at Southampton')
table6_filtered_male_southampton = table6_filtered_male.filter(lambda x: x['embarked'] == 'Southampton')
print(len(table6_filtered_male_southampton.table))
print()

my_DB.insert(table6)
my_table6 = my_DB.search('titanic')
my_pivot_0 = my_table6.pivot_table(['embarked', 'gender', 'class'], ['fare', 'fare', 'fare', 'last'], [lambda x: min(x), lambda x: max(x), lambda x: sum(x)/len(x), lambda x: len(x)])
print('Pivot table example:')
print(my_pivot_0)
print()

my_pivot_1 = table4.pivot_table(['position'], ['passes', 'shots'], [lambda x: sum(x)/len(x), lambda x: sum(x)/len(x)])
print('Pivot table test case#1')
print(my_pivot_1)
print()

my_pivot_2 = table2.pivot_table(['coastline', 'EU'], ['temperature', 'latitude', 'latitude'], [lambda x: sum(x)/len(x), lambda x: min(x), lambda x: max(x)])
print('Pivot table test case#2')
print(my_pivot_2)
print()

my_pivot_3 = table6.pivot_table(['class', 'gender', 'survived'], ['survived', 'fare'], [lambda x: len(x), lambda x: sum(x)/len(x)])
print('Pivot table test case#3')
print(my_pivot_3)
print()
