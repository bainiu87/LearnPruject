#encoding=utf-8
"""
动态规划 
"""


def best_floor(nums):
    left_sum = []
    
    s = 0
    for i in nums:
        s = s + i
        left_sum.append(s)

    lw_sum = [0]
    for i in xrange(1,  len(nums)):
        lw_sum.append(  lw_sum[-1] + left_sum[i-1]  )


    
    right_sum = []
    s = 0
    for i in nums[::-1]:
        s += i
        right_sum.append(s)

    rw_sum = [0]
    rge = range( len(nums) -1 )
    rge.reverse()
    for i in rge:
        rw_sum.append( rw_sum[-1]    + right_sum[i+1] )
    rw_sum.reverse()

    
    

    best_sum  =  1e6
    best_i =  0 #楼层
    for i in xrange( len(nums)):

        if lw_sum[i] + rw_sum[i] < best_sum:
            best_sum =  lw_sum[i] + rw_sum[i]
            best_i =  i

    return best_sum,best_i



print best_floor([1,2,3,2])








