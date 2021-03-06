package recfun
import common._

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
  def pascal(c: Int, r: Int): Int = {
    if (2 > r || 0 == c || r == c)
      1
    else
      pascal(c-1, r-1) + pascal(c, r-1)
  }

  /**
   * Exercise 2
   */
  def balance(chars: List[Char]): Boolean = {
    balanceWithCount(chars, 0, 0)
  }

  def balanceWithCount(chars: List[Char], lCount: Int, rCount: Int): Boolean = {
    if(rCount > lCount)
      false
    else if(chars.isEmpty) {
      if(lCount == rCount)
        true
      else
        false
    } else {
    	if (chars.head != '(' && chars.head != ')') {
    		balanceWithCount(chars.tail, lCount, rCount)
    	} else if (chars.head == '(') {
    	  balanceWithCount(chars.tail, lCount + 1, rCount)
    	} else {
    	  balanceWithCount(chars.tail, lCount, rCount + 1)
    	}
    }
  }
  /**
   * Exercise 3
   */
  def countChange(money: Int, coins: List[Int]): Int = {
    if(0 == money) 1
    else if(coins.isEmpty) 0
    else if(money < 0) 0
    else countChange(money, coins.tail) + countChange(money - coins.head, coins) //Don't use current coin + Use current coin
  }
}
