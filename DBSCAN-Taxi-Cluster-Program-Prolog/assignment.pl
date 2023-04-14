import:-
    csv_read_file('partition65.csv', Data65, [functor(partition)]),maplist(assert, Data65),
    csv_read_file('partition74.csv', Data74, [functor(partition)]),maplist(assert, Data74),
    csv_read_file('partition75.csv', Data75, [functor(partition)]),maplist(assert, Data75),
    csv_read_file('partition76.csv', Data76, [functor(partition)]),maplist(assert, Data76),
    csv_read_file('partition84.csv', Data84, [functor(partition)]),maplist(assert, Data84),
    csv_read_file('partition85.csv', Data85, [functor(partition)]),maplist(assert, Data85),
    csv_read_file('partition86.csv', Data86, [functor(partition)]),maplist(assert, Data86),listing(partition).

%Relabel returns a list that has its clusterids relabeled
%This function takes a value to be replaced, the item to replace it, The 2d list to perform the operation on, and the returning list. 
relabel(F,S,L,R):- relabel2(F,S,L,[],R).
relabel2(F,S,[],L,L).
relabel2(F,S,[H|T],L,R):- memberchk(F,H), replace(F,S,H,Z),add_tail(L,Z,L2), relabel2(F,S,T,L2,R).
relabel2(F,S,[H|T],L,R):- not(memberchk(F,H)), add_tail(L,H,L2), relabel2(F,S,T,L2,R).

%This function takes in a list of values to be replaced, the item to replace them, The list to perform the operation on, and the returning list. 
relabelList([],S,L,L).
relabelList([H|T],S,B,L):- relabel(H,S,B,R2), relabelList(T,S,R2,L). 

%This function takes a value to be replaced, the item to replace it, The 1d list to perform the operation on, and the returning list. 
replace(_, _, [], []).
replace(O, R, [O|T], [R|T2]) :- replace(O, R, T, T2).
replace(O, R, [H|T], [H|T2]) :- H \= O, replace(O, R, T, T2).

%The mergeCluster function takes a 2d list of points, and returns a merged list based on the definition seen in the assignment.
%mergeClusters calls the mergeClusters2 function, with an empty accumulator list.
mergeClusters(L,R):- set_prolog_stack(global, limit(100 000 000 000)), mergeClusters2(L,[],R).
%Base case that assigns the accumulator list to the returning list.
mergeClusters2([],A,A).

mergeClusters2([H|T],A,R):- add_tail(A,H,A2), %First, a point is added into the accumulator list.
                            nth0(1,H,X), nth0(2,H,Y), nth0(3,H,C2), %Second, we retrieve the X,Y value of the point, and then its clusterid.
                            getClusters(X,Y,A2,C), relabelList(C,C2,A2,R2), %Thirdly, we get the Clusterids that contain said point, and relabel the accumulator list, relabeling all the clusterids that cointain the point, to the most recent clusterid. 
                            mergeClusters2(T,R2,R). %Lastly, we recusivly call mergeClusters2 on the tail of our main list, to itterate through all the points.

%This function takes in a X and Y value, and returns a list of clusterids that contain the point.
getClusters(X,Y,L,R):-getClusters2(X,Y,L,[],R).
getClusters2(X,Y,[],L,L).
getClusters2(X,Y,[H|T],L,R):- H = [_,X,Y,_], nth0(3,H,A), add_tail(L,A,L2), getClusters2(X,Y,T,L2,R). 
getClusters2(X,Y,[H|T],L,R):- not(H = [_,_,X,Y,_]), getClusters2(X,Y,T,L,R). 

%This function takes in a list, and a value to be added to the tail of the list, and the resulting list.
add_tail([],X,[X]).
add_tail([H|T],X,[H|L]):-add_tail(T,X,L).

main(R):- findall([D,X,Y,C],partition(_,D,X,Y,C),L), mergeClusters(L,R).

test(relabelList) :- write('relabelList(33, 77,[[1,2.2,3.1,33], [2,2.1,3.1,22], [3,2.5,3.1,33], [4,2.1,4.1,33],[5,4.1,3.1,30]],Result)'),nl,
 relabelList([33,1], 77,
 [[1,2.2,3.1,33], [2,2.1,3.1,22], [3,2.5,3.1,33], [4,2.1,4.1,33], [5,4.1,3.1,30]],R),
 write(R).

 test(getClusters):- write('getClusters(40.750304,-73.952031,[[1345,40.750304,-73.952031,65000001],[6017,40.760146,-73.957873,65000002]],R)'),nl,
 getClusters(40.750304,-73.952031,[[1345,40.750304,-73.952031,65000001],[6017,40.760146,-73.957873,65000002]],R),
 write(R).

test(relabel):- write('relabel(1,2,[[1,2,3],[3,2,1]],R)'),nl,
 relabel(1,2,[[1,2,3],[3,2,1]],R),
 write(R).

test(replace):- write('replace(1,2,[1,2,3],R)'),nl,
 replace(1,2,[1,2,3],R),
 write(R).


