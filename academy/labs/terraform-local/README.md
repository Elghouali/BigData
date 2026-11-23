# Lab 04 — Infrastructure as Code: State, Plan, Apply and Drift

**Professional ticket:** Production was manually configured and cannot be reproduced. Prove the IaC workflow on a local resource before touching cloud accounts.

This Terraform exercise manages a local text file; it needs **no AWS account** and provisions no billable resources. Installation of Terraform and its local provider requires network access.

~~~bash
terraform init
terraform fmt -check
terraform validate
terraform plan
terraform apply
terraform plan
terraform destroy
~~~

The local output file and Terraform state are excluded from Git. Examine the plan before applying it.

## Ticket checklist

- [ ] Explain declarative state and the provider/resource model.
- [ ] Change the message variable and observe the plan.
- [ ] Manually edit the generated file: refresh/plan and explain drift.
- [ ] Destroy and confirm cleanup.
- [ ] Explain why deleting Terraform state is not equivalent to destroying a cloud resource.
- [ ] Draft a secure S3/GCS/Blob resource design without deploying.

**Cloud safety:** Review IAM, state encryption, budget and teardown before any future provider change.
