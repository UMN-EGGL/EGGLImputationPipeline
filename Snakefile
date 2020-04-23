import os
import pandas as pd

from snakemake.remote.S3 import RemoteProvider as S3RemoteProvider
from snakemake.remote.HTTP import RemoteProvider as HTTPRemoteProvider
from snakemake.remote.FTP import RemoteProvider as FTPRemoteProvider


S3 = S3RemoteProvider()

HTTP = HTTPRemoteProvider()
FTP = FTPRemoteProvider()

s3_key_id = os.environ.get('AWS_ACCESS_KEY')
s3_access_key = os.environ.get('AWS_SECRET_KEY')

S3 = S3RemoteProvider(
    endpoint_url='https://s3.msi.umn.edu', 
    access_key_id=s3_key_id, 
    secret_access_key=s3_access_key
)

configfile: "config.yaml"

rule all:
    input:
        # Phased
        S3.remote(
            expand(
                '{bucket}/data/vcfs/joint/imputed/MNEc2M:WGS/{fltr}/{maf}/VQSR{vqsr}/{sample}/{contig}/sample_imp.vcf.gz',
                bucket=config['bucket'],  caller=config['caller'], 
                maf=config['maf'],        contig=config['contigs'], 
                fltr=config['fltr'],      vqsr=config['vqsr'], 
                sample=config['samples']
            ),
            keep_local=True
        ),

#-----------------------------------------------------------------------------
#                           Step 01 - Filter GVCFs                          
#-----------------------------------------------------------------------------

rule filter_joint_bcftools_biallelic:
    input:
        gvcf = S3.remote(f'mccue-lab/ibiodatatransfer2019/joint_bcftools/{{contig}}.gvcf.gz',keep_local=True)
    output: 
        vcf = S3.remote('{bucket}/vcfs/{contig}/biallelic_{maf}_bcftools.vcf.gz',keep_local=True)
    resources:
        disk_gb = 100,
        mem_gb = 10
    params:
        max_mem =  '10G'
    run:
        if wildcards.maf == 'MAF01':
            min_af = '0.01'
        elif wildcards.maf == 'MAF005':
            min_af = '0.005'
        shell(f'''
            bcftools view -m2 -M2 -v snps,indels --min-af {min_af} {{input.gvcf}} -Ou | \
            bcftools norm -m+any -Ou | \
            bcftools sort -m {{params.max_mem}} -O z -o {{output.vcf}} 
        ''')

rule filter_joint_gatk_biallelic:
    input:
        gvcf = S3.remote(f'mccue-lab/ibiodatatransfer2019/joint_gatk/{{contig}}.gvcf.gz',keep_local=True)
    output: 
        vcf = S3.remote('{bucket}/vcfs/{contig}/biallelic_{maf}_gatk.vcf.gz',keep_local=True)
    resources:
        disk_gb = 500,
        mem_gb = 50
    params:
        max_mem =  '50G'
    run:
        if wildcards.maf == 'MAF01':
            min_af = '0.01'
        elif wildcards.maf == 'MAF005':
            min_af = '0.005'
        shell(f'''
            bcftools view -m2 -M2 -v snps,indels --min-af {min_af} {{input.gvcf}} -Ou | \
            bcftools norm -m+any -Ou | \
            bcftools sort -m {{params.max_mem}} -O z -o {{output.vcf}} 
        ''')


#-----------------------------------------------------------------------------
#                            Step 02 - Filter GVCFs                          
#-----------------------------------------------------------------------------


rule calculate_discordance:                                                     
    input:                                                                      
        imputed    = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_imp.vcf.gz',keep_local=True),
        ref        = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_ref.vcf.gz',keep_local=True),
    output:                                                                     
        concord    = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_concord.diff.indv',keep_local=True),
    params:                                                                     
        out        = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_concord',keep_local=True),
    shell:                                                                      
        f'''                                                                    
            vcftools \
                --gzvcf {{input.imputed}} \
                --gzdiff {{input.ref}} \
                --diff-indv-discordance \
                --out {{params.out}}
        '''          

rule impute_filtered_individual:
   input:
       ref_pop     = ancient(S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/ref_pop.vcf.gz',keep_local=True)),
       sample_flt  = ancient(S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_flt.vcf.gz',keep_local=True)),
   output:
       imputed     = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_imp.vcf.gz',keep_local=True),
       imp_log     = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_imp.log',keep_local=True),
   params:
       prefix      = f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_imp',
       heap_size   = '20g',
       window_size = 1,
       overlap     = 0.01,
       nthreads    = 2,
       timeout     = '120m'
   resources:
       mem_GB      = 20
   shell:
       f'''
           timeout {{params.timeout}} java -jar $BEAGLE_JAR \
               gt={{input.sample_flt}} \
               ref={{input.ref_pop}} \
               impute=true \
               nthreads={{params.nthreads}} \
               out={{params.prefix}} \
               window={{params.window_size}} \
               overlap={{params.overlap}}
       '''

rule filter_individual_for_imputation:
    input:
        phase  = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED.vcf.gz',keep_local=True),
        phasei = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED.vcf.gz.csi',keep_local=True),
        passd  = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz',keep_local=True),
        passi  = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.csi',keep_local=True),
        lst    = S3.remote(f'{BUCKET}/data/lsts/MNEc2M.lst',keep_local=True) 
    output:
        ref_pop     = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/ref_pop.vcf.gz',keep_local=True),
        sample_ref  = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_ref.vcf.gz',keep_local=True),
        sample_flt  = S3.remote(f'{BUCKET}/data/vcfs/joint/imputed/MNEc2M:WGS/{{fltr}}/{{maf}}/VQSR{{vqsr}}/{{sample}}/{{contig}}/sample_flt.vcf.gz',keep_local=True),
    shell:
        f'''
            # split out sample from reference population
            bcftools view {{input.phase}} --samples ^{{wildcards.sample}} -Oz -o {{output.ref_pop}}

            # split out the ORIGINAL sample to filter and for concordance later
            bcftools view {{input.passd}} --samples  {{wildcards.sample}} -Oz -o {{output.sample_ref}}
            bcftools index {{output.sample_ref}}

            # Down sample the single sample and remove the phase
            bcftools view {{output.sample_ref}} -R {{input.lst}} -Oz -o {{output.sample_flt}}
        '''

rule phase_VQSRPassed_vcf:
    input:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz',keep_local=True),
        idx = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.csi',keep_local=True),
    output:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED.vcf.gz',keep_local=True),
    params:
        prefix = f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PHASED',
        window = 0.05,
        overlap = 0.005,
        mem = '50g'
    resources:
        mem_gb = 50
    shell:
        '''
            watchdog \
                --gt {input.vcf} \
                --out-prefix {params.prefix}  \
                --heap-size {params.mem} \
                --window {params.window} \
                --overlap {params.overlap} 
        '''

rule combine_gatk_bcftools_vcfs:
    input:
        gatk_vcf     = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz', keep_local=True),
        gatk_idx     = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.tbi', keep_local=True),
        bcftools_vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz', keep_local=True),
        bcftools_idx = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.tbi', keep_local=True),
        fna          = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',
        fai          = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',
        fdict        = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.dict'
    output:
        merged_bare                = temp(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/MERGED_BARE.vcf.gz'),
        merged_partial_annotated   = temp(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/MERGED_PARTIAL_ANNOTATED.vcf.gz'),
        merged_fully_annotated     = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz',keep_local=True),
        merged_fully_annotated_idx = S3.remote(f'{BUCKET}/data/vcfs/joint/merged/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz.idx',keep_local=True)
    shell:
        f'''
        java -jar $GATK3_JAR \
            -T CombineVariants \
            -R {{input.fna}} \
            --variant:gatk {{input.gatk_vcf}} \
            --variant:bcftools {{input.bcftools_vcf}} \
            -genotypeMergeOptions PRIORITIZE \
            -priority gatk,bcftools | \
        bcftools annotate -x ^INFO/set,^FORMAT/GT -Oz -o {{output.merged_bare}}
        echo "Indexing {{output.merged_bare}}"
        bcftools index {{output.merged_bare}}

        echo "Adding GATK annotations to {{output.merged_bare}}" 
        bcftools annotate {{output.merged_bare}} -a {{input.gatk_vcf}} -c INFO/VQSLOD -Oz -o {{output.merged_partial_annotated}}
        bcftools index {{output.merged_partial_annotated}}

        echo "Addingt BCFTOOLS annotations to {{output.merged_partial_annotated}}"
        bcftools annotate {{output.merged_partial_annotated}} -a {{input.bcftools_vcf}} -c +INFO/VQSLOD -Oz -o {{output.merged_fully_annotated}} 
        bcftools index {{output.merged_fully_annotated}} -o {{output.merged_fully_annotated_idx}}
        ''' 

rule filter_VQSR_passed:
    input:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/ALL.vcf.gz',keep_local=True),
    output:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/PASS.vcf.gz',keep_local=True),
    shell:
        '''
            bcftools view -f PASS {input.vcf} -Ou |\
            bcftools annotate -x ^INFO/VQSLOD,^FORMAT/GT -O z -o {output.vcf}
        '''

rule apply_VQSR_filters_vcf:
    input:
        # VCF 
        vcf    = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz',keep_local=True),
        vcfidx = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz.tbi',keep_local=True),
        # Recalibration input
        recal     = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal',keep_local=True),
        recalidx  = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.idx',keep_local=True),
        plots     = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots',keep_local=True),
        plotspdf  = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots.pdf',keep_local=True),
        tranches  = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches',keep_local=True),
        tranchpdf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches.pdf',keep_local=True),
        # Refgen 
        fna    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',keep_local=True),
        fai    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',keep_local=True),
        fdict  = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict',keep_local=True)
    output:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/VQSR{{vqsr}}/ALL.vcf.gz',keep_local=True),
    run:
        vqsr = int(wildcards.vqsr)
        if vqsr > 100:
            vqsr = vqsr / 10
        else:
            vqsr = vqsr
        shell(f'''
            gatk ApplyVQSR \
            -R {{input.fna}} \
            -V {{input.vcf}} \
            --truth-sensitivity-filter-level {vqsr} \
            --tranches-file {{input.tranches}} \
            --recal-file {{input.recal}} \
            -mode SNP \
            -O {{output.vcf}} 
        ''')

rule recalibrate_bcftoolk_vcf:
    input:
        vcf    = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz',keep_local=True),
        vcfidx = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz.tbi',keep_local=True),
        mnec2m = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/MNEc2M.lst.vcf.gz',keep_local=True),
        index  = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/MNEc2M.lst.vcf.gz.tbi',keep_local=True),
        fna    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',keep_local=True),
        fai    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',keep_local=True),
        fdict  = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict',keep_local=True)
    output:
        recal     = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal',keep_local=True),
        recalidx  = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.idx',keep_local=True),
        plots     = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots',keep_local=True),
        plotspdf  = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots.pdf',keep_local=True),
        tranches  = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches',keep_local=True),
        tranchpdf = S3.remote(f'{BUCKET}/data/vcfs/joint/bcftools/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches.pdf',keep_local=True),
    shell:
        '''
            gatk VariantRecalibrator \
            -R {input.fna} \
            -V {input.vcf} \
            --resource:MNEc2M,known=false,training=true,truth=true,prior=15.0 {input.mnec2m} \
            -an DP \
            -an RPB \
            -an MQB \
            -an BQB \
            -an MQSB \
            -an SGB \
            -mode SNP \
            --max-gaussians 4 \
            -tranche 100.0 -tranche 99.9 -tranche 99.5 -tranche 99.0 -tranche 95.0 \
            -O {output.recal} \
            --tranches-file {output.tranches} \
            --rscript-file  {output.plots}
        '''    

rule recalibrate_gatk_vcf:
    input:
        vcf    = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz',keep_local=True),
        vcfidx = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz.tbi',keep_local=True),
        mnec2m = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/MNEc2M.lst.vcf.gz',keep_local=True),
        index  = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/MNEc2M.lst.vcf.gz.tbi',keep_local=True),
        fna    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',keep_local=True),
        fai    = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',keep_local=True),
        fdict  = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict',keep_local=True)
    output:
        recal     = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal',keep_local=True),
        recalidx  = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.idx',keep_local=True),
        plots     = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots',keep_local=True),
        plotspdf  = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.plots.pdf',keep_local=True),
        tranches  = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches',keep_local=True),
        tranchpdf = S3.remote(f'{BUCKET}/data/vcfs/joint/gatk/{{fltr}}/{{maf}}/{{contig}}/ALL.recal.tranches.pdf',keep_local=True),
    shell:
        '''
            gatk VariantRecalibrator \
            -R {input.fna} \
            -V {input.vcf} \
            --resource:MNEc2M,known=false,training=true,truth=true,prior=15.0 {input.mnec2m} \
            -an DP \
            -an QD \
            -an MQ \
            -an ReadPosRankSum \
            -an FS \
            -an SOR \
            -mode SNP \
            --max-gaussians 4 \
            -tranche 100.0 -tranche 99.9 -tranche 99.5 -tranche 99.0 -tranche 95.0 \
            -O {output.recal} \
            --tranches-file {output.tranches} \
            --rscript-file  {output.plots}
        '''    

rule subset_ALL_vcf:
    input:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz',keep_local=True),
        idx = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/ALL.vcf.gz.csi',keep_local=True),
        lst = S3.remote(f'{BUCKET}/data/lsts/{{lst}}.lst',keep_local=True)
    output: 
        snps=S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{lst}}.lst.vcf.gz',keep_local=True)
    shell:
        '''
            bcftools view {input.vcf} -R {input.lst} -o {output.snps} -O z    
        '''

rule create_vcf_tbi:
    input:
        vcf   = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{lst}}.vcf.gz',keep_local=True)
    output:
        index = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{lst}}.vcf.gz.tbi',keep_local=True)
    shell:  
        '''
            gatk IndexFeatureFile -I {input.vcf} -O {output.index}
        '''

rule create_vcf_csi:
    input:
        vcf = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{xxx}}.vcf.gz',keep_local=True)
    output:
        idx = S3.remote(f'{BUCKET}/data/vcfs/joint/{{caller}}/{{fltr}}/{{maf}}/{{contig}}/{{xxx}}.vcf.gz.csi',keep_local=True)
    shell:
        '''
            bcftools index {input.vcf} -o {output.idx}
        '''


#-----------------------------------------------------------------------------#
#-                           Create Legacy SNP lists                         -#
#-----------------------------------------------------------------------------#

rule make_snp_lsts:
    '''
        This rule creates LST files for SNPS on each of the current (MNEc) 
        and past (SNP50/70) arrays. It uses the remapped SNPs from Beeson et al.

        Wildcard good for: LST = [MNEc2M, MNEc670k, SNP70, SNP50]
    '''
    input:
        # strins in parens get auto concatenated in Python
        snps = HTTP.remote(
            'https://www.animalgenome.org/repository/pub'
            '/UMN2018.1003/{LST}.unique_remap.FINAL.csv.gz'
        ),
        assembly = FTP.remote(
            'ftp://ftp.ncbi.nlm.nih.gov/genomes/all/GCF/002/'
            '863/925/GCF_002863925.1_EquCab3.0/'
            'GCF_002863925.1_EquCab3.0_assembly_report.txt'
        )
    output:
        lst = S3.remote(f'{BUCKET}/data/lsts/{{LST}}.lst',keep_local=True)
    run:
        import pandas as pd
        # Read in the assembly mapping for chromosome names 
        # (since they are NC_XXXXX here and chrXX in beeson et al.)
        assembly = pd.read_table(input.assembly ,comment='#', header=None) 
        chrmap =  {x:y for x,y in zip(assembly[0].values, assembly[6].values)}
        # NOTE: I have NO idea why the HTTP remote returns a list ...
        # this could be a bug and might get fixed in newer versions of SnakeMake
        df = pd.read_table(input.snps[0],sep=',')
        
        def map_chrom(x): 
            '''
                Maps the chromosome name in the Beeson et al. file to the Ec3 RefSeq file
            '''
            if x in chrmap:
                return chrmap[x]
            elif x.startswith('chrUn_ref'):
                return x.replace('chrUn_ref|','').replace('|','')
            else:
                raise ValueError('Could not map chromosome name')
        
        # Create a DF with two columns: chromosome and position 
        df['CHR'] = [map_chrom(x) for x in df.EC3_chrom]
        df['POS'] = df.EC3_pos
            
        # Output the LSTs file
        df.loc[:,['CHR','POS']].to_csv(output.lst,index=False,header=None,sep='\t')


#-----------------------------------------------------------------------------#
#-                            Move Reference Files                           -#
#-----------------------------------------------------------------------------#

rule download_reference_files:
    '''
        This downloads the FNA files to the local filestystem so that it isn't
        downloaded for every rule that is run.
    '''
    input:
        fna   = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',keep_local=True),
        fai   = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',keep_local=True),
        fdict = S3.remote(f'{BUCKET}/data/fna/GCF_002863925.1_EquCab3.0_genomic.dict',keep_local=True)
    output:
        fna   = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.fna',
        fai   = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.fna.fai',
        fdict = f'data/fna/GCF_002863925.1_EquCab3.0_genomic.dict'
    shell:
        '''
	        cp {input.fna} {output.fna}
	        cp {input.fai} {output.fai}
	        cp {input.fdict} {output.fdict}
        '''

rule move_ref_genome_dict:
    '''
        In GATK, you specify the FNA file and the program assumes the name of 
        the index files. So this rule just copies the index file we have into 
        the path expected by GATK based on its name. 
    '''
    input:
        fdict  = S3.remote(f'{BUCKET}/fna/GCF_002863925.1_EquCab3.0_genomic.fna.dict',keep_local=True)
    output:
        fdict  = S3.remote(f'{BUCKET}/fna/GCF_002863925.1_EquCab3.0_genomic.dict',keep_local=True)
    shell:
        'cp {input[0]} {output[0]}'
