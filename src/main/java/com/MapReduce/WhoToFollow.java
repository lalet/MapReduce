package com.MapReduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.function.Predicate;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.mapred.lib.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.conf.Configuration;

/*
* Who To Follow is a map reduce program written in java to find out the people whom you may follow for a specific set of users
* and their followers specified in the input.
*/
public class WhoToFollow{

/**********

	Mapper  

	*********/
	
	
//AllPairsMapper emits key values pairs
//One with user and already following accounts with negative value
//Eg : (3, -2)
//Another set of pairs with following account id as the key and the the user account id as the value.
//Eg: (2, 3)
public static class AllPairsMapper extends Mapper<Object,Text,IntWritable,IntWritable>{
	public void map(Object key,Text values,Context context) throws IOException,InterruptedException{
		//Convert the string to tokens
		StringTokenizer st = new StringTokenizer(values.toString());
		//Parse the key
		IntWritable user=new IntWritable(Integer.parseInt(st.nextToken()));
		//IntWritable context for the account which the user follows
		IntWritable userFollowsAccount = new IntWritable();
		// First, go through the list of all the users the 'user' is following and emit
       // (user,-userFollowsAccount) to keep track of the accounts which the user already follows.
       // 'userFollowsAccount' will be used in the emitted pair
		while(st.hasMoreTokens()){
		      Integer account=Integer.parseInt(st.nextToken());
			  userFollowsAccount.set(account);
			  context.write(userFollowsAccount,user);
			  userFollowsAccount.set(-account);
			  context.write(user,userFollowsAccount);
		}
	}
}

//PairsReducer is used for creating the key values pair so that the negative values are carried forward to the next map
//and hence to remove the already following accounts from being suggested again.
public static class PairsReducer extends Reducer<IntWritable,IntWritable,IntWritable,Text>{
	public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
		IntWritable user=key;
		StringBuffer sb = new StringBuffer("");
		//Going through each value in the list of values and appending it as a string
		 while (values.iterator().hasNext()) {
	            Integer value = values.iterator().next().get();
	                // Using a StringBuffer is more efficient than concatenating strings
	                sb.append(value.toString() + " ");
	                }
	                Text result = new Text(sb.toString());
	                context.write(user, result);   
	            } 
	        }
/**********

Mapper for filtering 
the already followed 
accounts by an user

*********/

public static class AllPairsMapperWithFilter extends Mapper<Object,Text,IntWritable,IntWritable>{
	
	public void map(Object key,Text values,Context context) throws IOException,InterruptedException{
		StringTokenizer st=new StringTokenizer(values.toString());
		IntWritable user=new IntWritable(Integer.parseInt(st.nextToken()));
		// 'UserFollowsAccountsList' will store the list of accounts which the user follows
        ArrayList<Integer> userFollows = new ArrayList<Integer>();
        //IntWritable form of the userAccount which the user follows
        IntWritable userFollowsAccount = new IntWritable();
       //If the value is less than zero, the user is already following those
        //accounts and we emit them as negative values to keep track of already followed accounts by a particular user. 
       while(st.hasMoreTokens()){
        	Integer userFollowsAccountId = Integer.parseInt(st.nextToken());
        	if (userFollowsAccountId < 0){
        		userFollowsAccount.set(userFollowsAccountId);
        		context.write(user, userFollowsAccount);
        	}
        	else{
        		userFollows.add(userFollowsAccountId);
        	}
        	
        }
        
        //for each inverted list X, [ Y1, Y2, ..., Yk ] , 
        //mapper emits all pairs (Yi,Yj) and (Yj,Yi) where i belongs to [1,k] , j belongs to [i,k]
        ArrayList<Integer> seenAccountIds = new ArrayList<Integer>();
        IntWritable userFollowsAccount2 = new IntWritable();
        for (Integer accountId:userFollows){
        	userFollowsAccount.set(accountId);
        	for (Integer seenAccountId:seenAccountIds){
        		userFollowsAccount2.set(seenAccountId);
        		context.write(userFollowsAccount, userFollowsAccount2);
        		context.write(userFollowsAccount2, userFollowsAccount);
        	}
        	seenAccountIds.add(userFollowsAccount.get());
        }
        
		
	}
}

/**********

Reducer

*********/
public static class CountReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text>{
	
	//Recommendation class has all the methods to add the number of common accounts
	//Find if the accounts are already existing in the recommendations list, convert the values to 
	//to string etc.
	private static class Recommendation{
		//Attributes
		private Integer accountId;
		private Integer nCommonAccounts;
		
		//Constructor
		public Recommendation(Integer accountId){
			this.accountId=accountId;
			// A recommendation must have at least 1 common account they follow
            this.nCommonAccounts = 1;
		}
		
		// Getters
        public Integer getAccountId() {
            return accountId;
        }
        
        public Integer getNCommonAccounts() {
            return nCommonAccounts;
        }
        
        public void addCommonAccount(){
        	nCommonAccounts++;	
        }
        
        public String toString(){
        	return accountId + "(" + nCommonAccounts + ")";
        }
        
        public static Recommendation find(Integer accountId,ArrayList<Recommendation> recommendations){
        	for (Recommendation recommendedAccount:recommendations){
        		if (recommendedAccount.getAccountId() == accountId){
        			return recommendedAccount;
        		}
        	}
        	return null;
        }
        
        
	}

	public void reduce(IntWritable key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{

		IntWritable user=key;
		StringTokenizer st=new StringTokenizer(values.toString());
		// 'alreadyFollowing' will store the accounts which the  'user' is already following
        // (the negative values in 'values').
		//alreadyFollowing will hold the negative values list.
        ArrayList<Integer> alreadyFollowingList = new ArrayList<Integer>();
        //suggestedAccountsList will hold the account ids the user can follow
        ArrayList<Integer> suggestedAccountsList = new ArrayList<Integer>();
       
        
        while (values.iterator().hasNext()) {
            Integer value = values.iterator().next().get();
            if (value > 0) {
            	suggestedAccountsList.add(value);
            } else {
            	alreadyFollowingList.add(value);
            }
        }
        
        //Predicate used below removes all the values from suggested accounts list
        // if the corresponding negative value is appearing in alreadyFollows list.
        //This is in order to avoid already following accounts being suggested again.
        for (Integer alreadyFollowsAccount:alreadyFollowingList){
        	suggestedAccountsList.removeIf(new Predicate<Integer>(){
        		@Override
        		public boolean test(Integer t){
        			return t.intValue() == -alreadyFollowsAccount.intValue();
        		}
        	});
        	
        }
        
        ArrayList<Recommendation> recommendations = new ArrayList<Recommendation>();
        for (Integer accountId:suggestedAccountsList){
        	Recommendation account = Recommendation.find(accountId, recommendations);
        	if (account == null){
        		recommendations.add(new Recommendation(accountId));
        	}
        	else{
        		 account.addCommonAccount();
        	}
        }
        
        recommendations.sort(new Comparator<Recommendation>(){
        	@Override
        	public int compare(Recommendation t,Recommendation t1){
        		return -Integer.compare(t.getNCommonAccounts(), t1.getNCommonAccounts());
        	}
        });
        
        // Builds the output string that will be emitted
        // The top ten accounts according to the number of common followers will be suggested for each user
        StringBuffer sb = new StringBuffer("");
        for (int i = 0; i < recommendations.size() && i < 10; i++) {
            Recommendation recommendedAccount = recommendations.get(i);
            sb.append(recommendedAccount.toString() + " ");
        }
        Text result = new Text(sb.toString());
        context.write(user, result);    
               
	}
}





public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
	try {
		Configuration conf1 = new Configuration();
        Job job1 = new Job(conf1, "Who To Follow");
        job1.setJarByClass(WhoToFollow.class);
        job1.setMapperClass(AllPairsMapper.class);
        job1.setReducerClass(PairsReducer.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("intermediate"));
        job1.waitForCompletion(true);
        Configuration conf2 = new Configuration();
        Job job2 = new Job(conf2, "Who To Follow");
        job2.setJarByClass(WhoToFollow.class);
        job2.setMapperClass(AllPairsMapperWithFilter.class);
        job2.setReducerClass(CountReducer.class);
        job2.setOutputKeyClass(IntWritable.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("intermediate"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
        
    } catch (Exception e) {
    	e.printStackTrace();
    }
   }


}

