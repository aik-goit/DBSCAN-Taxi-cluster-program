In order to run the predicate to make the 
list, you need to create the main list, and 
then call the mergeClusters, and create file like such:

?- import. 
?- findall([D,X,Y,C],partition(_,D,X,Y,C),L), mergeClusters(L,R),open('clusters4.txt',write,F),write(F,R),close(F).

If you would like to see the list in order:
?- findall([D,X,Y,C],partition(_,D,X,Y,C),L), sort(L,L2),mergeClusters(L2,R),open('clusters.txt',write,F),write(F,R),close(F).