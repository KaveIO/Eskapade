#! /bin/bash

# dataset from:
# https://rajmak.wordpress.com/2013/04/27/clustering-text-map-reduce-in-python/
 
tmp=$"Under Armour Performance Team Polo - Mens - For All Sports - Clothing - Red/White
adidas Adipure 11PRO TRX FG - Women's
Nike Therma-FIT K.O. (MLB Twins)
HI Nike Polo Girls Golf Dress
Converse All Star PC2 - Boys' Toddler
HI Nike Team (NFL Giants) BCA Womens T-Shirt
adidas Sprintstar 3 - Women's
Nike Therma-FIT K.O. (MLB Rays)
Converse All Star Ox - Girls' Toddler
adidas Sprintstar 4 - Men's
adidas adiPURE IV TRX FG - Men's
HI Nike Attitude (NFL Titans) BCA Womens T-Shirt
HI Nike Sport Girls Golf Dress
Under Armour Performance Team Polo - Mens - For All Sports - Clothing - Purple/White
Nike College All-Purpose Seasonal Graphic (Oklahoma) Womens T-Shirt
HI Nike College All-Purpose Seasonal Graphic (Washington) Womens T-Shirt
Nike Therma-FIT K.O. (MLB Phillies)
Brooks Nightlife Infiniti 1/2 Zip Jacket - Mens
Brooks Nightlife Infiniti 1/2 Zip - Women's
HI Nike Solid Girls Golf Shorts
"
echo "$tmp" > products.list

cmd="cat products.list | eskapade_run -L FATAL esk108_map.py | eskapade_run -L FATAL esk108_reduce.py"
echo "Now running command:"
echo "$cmd"
echo

# run 
cat products.list | eskapade_run -L FATAL esk108_map.py | eskapade_run -L FATAL esk108_reduce.py

# clean-up
rm products.list

