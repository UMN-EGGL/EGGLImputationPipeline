import os
import pandas as pd

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider
from snakemake.remote.FTP import RemoteProvider as FTPRemoteProvider

s3_key_id = os.environ.get('AWS_ACCESS_KEY')
s3_access_key = os.environ.get('AWS_SECRET_KEY')

S3 = S3RemoteProvider(
    endpoint_url='https://s3.msi.umn.edu', 
    access_key_id=s3_key_id, 
    secret_access_key=s3_access_key
)

HTTP = HTTPRemoteProvider()
FTP = FTPRemoteProvider()

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
    #'snps_indels',
    'biallelic_snps'
]

lsts = ['MNEc2M', 'MNEc670k', 'SNP70', 'SNP50']

rule all:
    input:
        # Create the SNP LSTs
        S3.remote(expand('mccue-lab/Ec3Genomes/data/lsts/{LST}.lst',LST=lsts))
        # Create phased VCFs
        #S3.remote(expand('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.phased.vcf.gz',contig=contigs,caller=caller,feature=feature))

rule phase_vcf:
    input:
        snps=S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.vcf.gz')
    resources:
        phase_jobs=1
    threads: 32
    output:
        phased=S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.phased.vcf.gz')
    params:
        prefix='mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.{feature}.phased'
    shell: 
        '''
            java -Xmx110g -jar .local/src/beagle.jar gt={input.snps} out={params.prefix} impute=true nthreads={threads} window=10 overlap=1
        '''

rule filter_bi_allelic_SNP_joint_vcf:
    input:
        gvcf=S3.remote('mccue-lab/ibiodatatransfer2019/joint_{caller}/{contig}.gvcf.gz')
    output: 
        snps=S3.remote('mccue-lab/Ec3Genomes/data/vcfs/joint/{caller}/{contig}.biallelic_snps.vcf.gz')
    shell:
        '''
            .local/bin/bcftools view -m2 -M2 -v snps {input.gvcf} -o {output.snps} -O z    
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

rule make_snp_lsts:
    '''
    good for: LST = [MNEc2M, MNEc670k, SNP70, SNP50]
    '''
    input:
        snps = HTTP.remote('https://www.animalgenome.org/repository/pub/UMN2018.1003/{LST}.unique_remap.FINAL.csv.gz'),
        assembly = FTP.remote('ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/002/863/925/GCF_002863925.1_EquCab3.0/GCF_002863925.1_EquCab3.0_assembly_report.txt')
    output:
        lst = S3.remote('mccue-lab/Ec3Genomes/data/lsts/{LST}.lst')
        #lst = 'mccue-lab/Ec3Genomes/data/lsts/{LST}.lst'
    run:
        import pandas as pd
        # Read in the assembly mapping
        assembly = pd.read_table(input.assembly ,comment='#', header=None) 
        chrmap =  {x:y for x,y in zip(assembly[0].values, assembly[6].values)}
        # I have NO idea why the HTTP remote returns a list ...
        df = pd.read_table(input.snps[0],sep=',')
        
        def map_chrom(x): 
            'maps the chromosome name in the beeson et al file to the Ec3 RefSeq file'
            if x in chrmap:
                return chrmap[x]
            elif x.startswith('chrUn_ref'):
                return x.replace('chrUn_ref|','').replace('|','')
            else:
                raise ValueError
        
        # Add the refeseq chromosomes names to each  

        df['CHR'] = [map_chrom(x) for x in df.EC3_chrom]
        df['POS'] = df.EC3_pos

        df.loc[:,['CHR','POS']].to_csv(output.lst,index=False,header=None,sep='\t')
        
         
        
