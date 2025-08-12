a = [1,2,3,4,5,6,7,8,9]


maxi = 0
numbers = []
for i in range(len(a)-2):
    for j in range(i+1,len(a)-1):
        if a[i]+a[j]+a[j+1]>maxi:
            maxi =a[i]+a[j]+a[j+1]
            numbers = [a[i],a[j],a[j+1]]
            
print(maxi,numbers)


          ####or###

b = sorted(a)[::-1]
print(sum(b[:3]),b[:3])
