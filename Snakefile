from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
s3_key_id = os.environ.get('AWS_ACCESS_KEY')
s3_access_key = os.environ.get('AWS_SECRET_KEY')

S3 = S3RemoteProvider(
    endpoint_url='https://s3.msi.umn.edu', 
    access_key_id=s3_key_id, 
    secret_access_key=s3_access_key
)

contigs = [
#    'NC_001640_1','NC_009144_3','NC_009145_3',                                        
#    'NC_009146_3','NC_009147_3','NC_009148_3',                                        
#    'NC_009149_3','NC_009150_3','NC_009151_3',                                        
#    'NC_009152_3','NC_009153_3','NC_009154_3',                                        
#    'NC_009155_3','NC_009156_3','NC_009157_3',                                        
#    'NC_009158_3','NC_009159_3','NC_009160_3',                                        
#    'NC_009161_3','NC_009162_3','NC_009163_3',                                        
#    'NC_009164_3','NC_009165_3','NC_009166_3',                                        
#    'NC_009167_3','NC_009168_3','NC_009169_3',                                        
#    'NC_009170_3','NC_009171_3','NC_009172_3',                                        
     'NC_009173_3','NC_009174_3','NC_009175_3',                                        
#     'unplaced'
] 
caller = [
    'gatk',
    'bcftools'
]
feature = [
    'snps_indels'
]

rule all:
    input:
        S3.remote(expand('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.phased.vcf.gz',contig=contigs,caller=caller,feature=feature))

rule phase_vcf:
    input:
        snps=S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.vcf.gz')
#    resources:
#        phase_jobs=1
    threads: 31
    output:
        phased=S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.phased.vcf.gz')
    shell: 
        '''
            java -Xmx200g -jar .local/src/beagle.jar gt={input.snps}  out={output.phased} impute=true nthreads={threads} window=5 overlap=1
        '''

rule filter_SNP_INDELS_joint_vcf:
    input:
        gvcf=S3.remote('mccue-lab/ibiodatatransfer2019/joint_{caller}/{contig}.gvcf.gz')
    output: 
        snps=S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.snps_indels.vcf.gz')
    shell:
        '''
            .local/bin/bcftools view -m2 -v snps,indels {input.gvcf} -o {output.snps} -O z    
        '''
