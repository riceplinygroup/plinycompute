


% Recursively traverse the query graph:
bottomUpSearch(Node, Input, Projection, LeavesRemaining, ListofVisitedNodes):-

	% First we want to check: 
	% If the node has been visited before?
	
	(member(Node, ListofVisitedNodes) ->

		% This "Node" has been visited before.
		
		% Check if any leaf node is still unexplored.
		% Start search from next leaf node. 
		% Else we are done.
		
		length(LeavesRemaining, LengthList),
		(LengthList \= 0 -> 
			[L|Rest] = LeavesRemaining,
			bottomUpSearch(L, [], [], Rest, ListofVisitedNodes)
			;
			true
		)	
		; 
		
		% The node has not been visited before.

		% Add this node to the list of visited.
		append(ListofVisitedNodes, [Node], Visited),
		
		
		% Are we at root node?
		(Node = virtualRootNode ->
		
			% Start from next leaf node.
			length(LeavesRemaining, LengthList),
			(LengthList \= 0 -> 
				[L|Rest] = LeavesRemaining,
				bottomUpSearch(L, [], [], Rest, Visited)
				;
				true
			)
			;
			
			% Else go up and explore the next compute node:	
			
			node(Node,Y,CompName),

			% Scan Node: 
			(Y = scan -> 
				write(Node),
				link(Next,Node,Out,In,Project),
				write("("),printList(Out),write(")<= SCAN("), 
				scan(Node,SetName,DBName),
				printString(SetName),write(","),
				printString(DBName),write(","),
				printString(CompName), write(")"),nl, 
				bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			 ; true
			),
			
			
			
			% Apply Node:
			(Y = apply -> 
				link(Next,Node,Out,In,Project),
				apply(Node, InName, PrName, LName), 
				write(Node),
				write("("),printList(Out),write(")<= APPLY("), 
				printNameListPair(InName, Input), write(","), 
				printNameListPair(PrName, Projection), write(","),
				printString(CompName), write(","),
				printString(LName), 
				write(")"), nl, bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			 ; true
			 ),
 
			
			% HashLeft Node:
			(Y = hashleft -> 
				write(Node),
				link(Next,Node,Out,In,Project),
				write("("),printList(Out),write(")<= HASHLEFT("), 
				hashleft(Node, InName, PrName, LName), 
				printNameListPair(InName, Input), write(","), 
				printNameListPair(PrName, Projection), write(","),
				printString(CompName), write(","),
				printString(LName), 
				write(")"), nl, bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			 ; true
			 ),	
			 
			
			% HashRight Node:
			(Y = hashright -> 
				write(Node),
				link(Next,Node,Out,In,Project),
				write("("),printList(Out),write(")<= HASHRIGHT("), 
				hashright(Node, InName, PrName, LName), 
				printNameListPair(InName, Input), write(","), 
				printNameListPair(PrName, Projection), write(","),
				printString(CompName), write(","),
				printString(LName), 
				write(")"), nl, bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			 ; true
			 ),	

			
			% Join Node:
			(Y = join -> 
				link(Next,Node,Out,In,Project),
				write(Node),
				write("("),printList(Out),write(")<= JOIN("),
				join(Node, LeftInName, LeftPrName, RightInName, RightPrName),
				link(Node, LeftInName, _, LeftIn, LeftProject),
				link(Node, RightInName, _, RightIn, RightProject),
				printNameListPair(LeftInName, LeftIn), write(","), 
				printNameListPair(LeftPrName, LeftProject), write(","), 
				printNameListPair(RightInName, RightIn), write(","), 
				printNameListPair(RightPrName, RightProject), write(","),
				printString(CompName), 
				write(")"), nl, bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			; true	
			),
			
			
			% Filter Node:
			(Y = filter -> 
				write(Node),
				link(Next,Node,Out,In,Project),
				write("("),printList(Out),write(")<= FILTER("),
				filter(Node, InName, PrName),
				printNameListPair(InName, Input), write(","), 
				printNameListPair(PrName, Projection), write(","),
				printString(CompName),		
				write(")"), nl, bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			; true
			),
			

			% Hashone Node:
			(Y = hashone -> 
				write(Node),
				link(Next,Node,Out,In,Project),
				write("("),printList(Out),write(")<= HASHONE("),
				hashone(Node, InName, PrName),
				printNameListPair(InName, Input), write(","), 
				printNameListPair(PrName, Projection), write(","),
				printString(CompName),		
				write(")"), nl, bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			; true
			),
			
			
			% Flatten Node:
			(Y = flatten -> 
				write(Node),
				link(Next,Node,Out,In,Project),
				write("("),printList(Out),write(")<= FLATTEN("),
				flatten(Node, InName, PrName),
				printNameListPair(InName, Input), write(","), 
				printNameListPair(PrName, Projection), write(","),
				printString(CompName),		
				write(")"), nl, bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			; true
			),

			
			% Output Node: 
			(Y = output -> 
				write(Node),
				link(Next,Node,Out,In,Project),
				write("("),printList(Out),write(")<= OUTPUT("), 
				output(Node, InName, SetName,DBName),
				printNameListPair(InName, Input), write(","), 
				printString(SetName),write(","),
				printString(DBName),write(","),
				printString(CompName), write(")"),nl, 
				bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			 ; true
			),
			

			% Aggregate Node:
			(Y = aggregate -> 
				write(Node),
				link(Next,Node,Out,In,Project),
				write("("),printList(Out),write(")<= AGGREGATE("),
				aggregate(Node, InName),
				printNameListPair(InName, Input), write(","), 
				printString(CompName),		
				write(")"), nl, bottomUpSearch(Next, In, Project, LeavesRemaining, Visited)
			; true
			)			
		)
	).
	
	
	


% Find the leaves:
findLeaves(Leaves):-
	findall(X0, scan(X0,_,_), Leaves).
	

% TCAP generator:
tcapGenerator():-
	findLeaves([H|T]),
	bottomUpSearch(H, [], [], T, []).
	

% Print name-list pair: TCAP:
printNameListPair(ListName, List):-	
	write(ListName), write("("), printList(List), write(")").
	
	
% Print string to console: 'String'
printString(String):-
	write("'"),write(String),write("'").
	
% Print list to console:
printList([]).
printList([H|T]):-
	write(H),
	length(T, Len),
	(	Len \= 0 -> write(",") 
		; true
	),
	printList(T).
