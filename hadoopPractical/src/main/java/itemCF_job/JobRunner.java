package itemCF_job;

import itemCF.step1.MR1;
import itemCF.step2.MR2;
import itemCF.step3.MR3;
import itemCF.step4.MR4;
import itemCF.step5.MR5;

/**
 * @author Yuqi Li
 * date: Mar 24, 2018 3:46:38 PM
 */
public class JobRunner {
	public static void main(String[] args) {
		//每一步作业的状态
		int status1 = -1;
		int status2 = -1;
		int status3 = -1;
		int status4 = -1;
		int status5 = -1;
		
		status1 = new MR1().run();
		if(status1 == 1){
			System.out.println("Step1 success -> start step2");
			status2 = new MR2().run();
		}else{
			System.out.println("Step1 failed");
		}
		if(status2 == 1){
			System.out.println("Step2 success -> start step3");
			status3 = new MR3().run();
		}else{
			System.out.println("Step2 failed");
		}
		if(status3 == 1){
			System.out.println("Step3 success -> start step4");
			status4 = new MR4().run();
		}else{
			System.out.println("Step3 failed");
		}
		if(status4 == 1){
			System.out.println("Step4 success -> start step5");
			status5 = new MR5().run();
		}else{
			System.out.println("Step4 failed");
		}
		if(status5 == 1){
			System.out.println("The last step success");
		}else{
			System.out.println("Step5 failed");
		}
	}
}
