/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CassandraJoinsParser;

import java.util.ArrayList;

/**
 * TODO 1. PriorityQueue, with root + left childs -> Operators (AND,OR), with right child (clauses) [DONE]
 * TODO 2. Insert new clause [DONE]
 * TODO 3. Get join node function [DONE]
 * @author Alex
 */
public class ExpressionTree {
    private ArrayList<ExpressionTreeNode> exprTree;
    
    public ExpressionTree()
    {
        exprTree = new ArrayList<ExpressionTreeNode>();
    }
    
    public boolean insertClause(String clause, String op)
    {
        boolean insertionSuccessResult = true;
        ExpressionTreeNode clauseNode, operatorNode; 
        
        if (clause.isEmpty())
        {
            insertionSuccessResult = false;
            return insertionSuccessResult;
        }
        
        clauseNode = new ClauseNode(clause, checkForJoinClause(clause));
        operatorNode = new OperatorNode(op);
        
        if (! op.isEmpty())
        {
            if (exprTree.isEmpty())
            {
                operatorNode.setRightChild(clauseNode);
            }
            else
            {
                ExpressionTreeNode currentNode = traverseExpressionTree();

                operatorNode.setRightChild(clauseNode);

                currentNode.setLeftChild(operatorNode);
                currentNode.setRightChild(clauseNode);
            }

            exprTree.add(operatorNode);
            exprTree.add(clauseNode);
        }
        else
        {
            if (exprTree.isEmpty())
            {
                exprTree.add(clauseNode);
            }
            else
            {
                ExpressionTreeNode currentNode = traverseExpressionTree();
                
                currentNode.setLeftChild(clauseNode);
                
                exprTree.add(clauseNode);
            }
        }
        
        return insertionSuccessResult;
    }
    
    protected boolean checkForJoinClause(String clause)
    {
        return clause.matches(".*\\..*=.*\\..*");
    }
    
    protected ExpressionTreeNode traverseExpressionTree()
    {
        ExpressionTreeNode lastLeftDeepNode = exprTree.get(0);
        
        while(lastLeftDeepNode.getLeftChild() != null)
        {
            lastLeftDeepNode = lastLeftDeepNode.getLeftChild();
        }
        
        return lastLeftDeepNode;
    }
    
    protected void printTree()
    {
        exprTree.forEach(node -> 
        {
            if (node instanceof ClauseNode)
            {
                System.out.println("Node Clause: " + ((ClauseNode) node).getClause() + " Is join clause: " + ((ClauseNode) node).getIsJoin());
                
            }
            else if (node instanceof OperatorNode)
            {
                System.out.println("Node Clause: " + ((OperatorNode) node).getOperator());
            }
            else
            {
                System.out.println("Undefined Node Found... " + node.getClass().getName());
            }
        });
    }
    
    protected String getJoinFields()
    {
        String result = "";
        
        for (ExpressionTreeNode node : exprTree)
        {
            if (node instanceof ClauseNode)
            {
                if (((ClauseNode) node).getIsJoin())
                {
                    result = ((ClauseNode) node).getClause();
                }
            }
        }

        return result;
    }
    
    protected ArrayList<ExpressionTreeNode> getExpressionTree()
    {
        return exprTree;
    }
    
    protected class ExpressionTreeNode
    {
        private ExpressionTreeNode _left, _right;
        
        protected ExpressionTreeNode() 
        {
            _right = null;
            _left = null;
        }
        
        protected void setLeftChild(ExpressionTreeNode left)
        {
            _left = left;
        }
        
        protected void setRightChild(ExpressionTreeNode right)
        {
            _right = right;
        }
        
        protected ExpressionTreeNode getLeftChild()
        {
            return _left;
        }
        
        protected ExpressionTreeNode getRightChild()
        {
            return _right;
        }
    }
    
    protected class ClauseNode extends ExpressionTreeNode
    {
        private final String _clause;
        private final boolean _isJoinFlag;
        
        protected ClauseNode(String clause, boolean isJoinFlag)
        {
            super();
            _clause = clause;
            _isJoinFlag = isJoinFlag;
        }
        
        protected boolean getIsJoin()
        {
            return _isJoinFlag;
        }
        
        protected String getClause()
        {
            return _clause;
        }
    }
    
    protected class OperatorNode extends ExpressionTreeNode
    {
        private final String _operator;
        
        protected OperatorNode(String operator)
        {
            super();
            _operator = operator;
        }
        
        protected String getOperator()
        {
            return _operator;
        }
    }
}
